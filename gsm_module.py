#!/usr/bin/env python3

# System Imports
from datetime import (
    datetime,
    timezone
)
import json
import pika
import serial
from time import sleep

# Framework / Library Imports

# Application Imports

# Local Imports
import config


def send_at_command(command, append_eol=True, encoding='iso8859_2'):
    command = command + "\r\n" if append_eol else command
    port.write(command.encode(encoding))
    return list(map(lambda elem: elem.decode(encoding, errors="replace"), port.readlines()))


def init(pin=None):
    while True:
        result = send_at_command("ATI")
        if len(result) > 0 and result[-1] == "OK\r\n":
            break

    if (not enter_pin(pin)):
        raise Error("PIN authentification has failed!")

    # switch to text mode so commands look nicer
    send_at_command("AT+CMGF=1")

    # store received sms on sim card
    # i.e. disable cnmi notifications and set storage
    # of newly arrived messages to gsm module memory
    send_at_command("AT+CNMI=0,0,0,0,0")
    send_at_command("AT+CPMS=\"ME\",\"ME\",\"ME\"")

    print("GSM module initialized!")


def enter_pin(pin=None):
    pin_status = send_at_command("AT+CPIN?")[2]

    if pin_status == "+CPIN:READY\r\n":
        return True
    elif pin_status == "+CPIN:SIM PIN\r\n":
        auth_result = send_at_command("AT+CPIN=\"" + pin + "\"")
        return auth_result[2] == "OK\r\n"
    else:
        return False


def send_sms_message(phone_number, text):
    assert phone_number.startswith("+44")

    command_sequence = [
        "AT+CMGF=1",
        "AT+CMGS=" + phone_number,
        text
    ]

    for command in command_sequence:
        send_at_command(command)

    result = send_at_command(chr(26), False)
    print(result)


def get_sms_messages(category="ALL"):
    assert category in [
        "ALL", "REC READ", "REC UNREAD", "STO UNSENT", "STO SENT"
    ]

    messages = []
    response_raw = send_at_command("AT+CMGL=" + category)

    # print(response_raw)

    sms_list_raw = response_raw[2:-2]
    # the odd elements are sms metadata, the even ones are sms texts
    sms_pairs = zip(sms_list_raw[0::2], sms_list_raw[1::2])

    for sms_meta, sms_text in sms_pairs:
        messages.append(parse_sms(sms_meta, sms_text))

    return messages


def delete_all_sms_messages():
    sms_messages_to_delete = get_sms_messages("ALL")

    for sms_message in sms_messages_to_delete:
        delete_sms_message(sms_message["index"])


def delete_sms_message(index):
    return send_at_command("AT+CMGD=" + str(index))


def parse_sms(sms_meta, sms_text):
    sms_meta = sms_meta.split(',')

    return {
        'index': int(sms_meta[0].split(': ')[1]),
        'category': sms_meta[1].split("\"")[1],
        'sender': sms_meta[2].split("\"")[1],
        'datetime': str(format_dtstr_to_obj(clean_datetime(sms_meta[4] + " " + sms_meta[5]))),
        'timestamp': format_dtstr_to_obj(clean_datetime(sms_meta[4] + " " + sms_meta[5])).replace(tzinfo=timezone.utc).timestamp(),
        'text': sms_text.rstrip()
    }


def clean_datetime(dt_string):
    dt_string = dt_string.rstrip() # Remove the new line chars
    if dt_string.startswith('"'):
        dt_string = dt_string[1:] # Remove starting "
    if dt_string.endswith('"'):
        dt_string = dt_string[0:-1] # Remove ending "
    if dt_string.endswith("+00"):
        dt_string = dt_string+"00"

    return dt_string


def format_dtstr_to_obj(dt_string):
    return datetime.strptime(dt_string, '%Y/%m/%d %H:%M:%S%z')


def get_phonebook(begin=1, end=250):
    response = send_at_command('AT+CPBR=1,250')
    result = list(map(parse_raw_phonebook_entry, response[2:-3]))

    return result


def parse_raw_phonebook_entry(entry):
    entry = entry[entry.find('+CPBR: ') + 7:]
    entry = entry.split(',')
    return {
        'id': int(entry[0]),
        'number': entry[1][1:-2],
        'type': int(entry[2]),
        'name': entry[3][1:-3]    
    }


def save_phonebook_to_file(filename='contacts.json'):
    phonebook = get_phonebook()

    with open(filename, 'w') as outfile:
        json.dump(phonebook, outfile)


def load_phonebook_from_file(filename='contacts.json'):
    with open('contacts.json') as f:
        phonebook = json.load(f)

    for entry in phonebook:
        # print(send_at_command('AT+CPBW=' + str(entry['id'])))
        print(send_at_command(''.join((
            'AT+CPBW=',
            str(entry['id']),
            ',\"', entry['number'], '\",',
            str(entry['type'] + 1 if entry['type'] % 2 == 0 else entry['type']),
            ',\"', entry['name'].replace(';/O\"', '').replace('/M\"', '').replace(';', ' ').replace('\"',''), '\"'
        ))))
        print(entry['id'])


def watch_serial_port():
    print("Listening to port...")
    while True:
        received_data = port.read()              #read serial port
        sleep(0.03)
        data_left = port.inWaiting()             #check for remaining byte
        received_data += port.read(data_left)
        if len(received_data) > 0:
            line_data = received_data.decode("iso8859_2")
            # print (line_data)    #print received data

            if '+CIEV: "MESSAGE"' in line_data:
                print("Inbound Message!")
                collect_and_push_to_rabbit(get_sms_messages())
        # port.write(received_data)                #transmit data serially 


def get_rabbit_connection(user, password, server):
    credentials = pika.PlainCredentials(
        config.RABBIT_USER, 
        config.RABBIT_PASS
    )
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            config.RABBIT_SERVER,
            5672,
            '/',
            credentials
        )
    )
    return connection.channel()


def collect_and_push_to_rabbit(messages):
    for message in messages:
        try:
            # print("Timestamp: " + str(message['timestamp']))
            # print(str(message))
            channel.basic_publish(
                exchange='',
                routing_key=config.RABBIT_QUEUE,
                body=json.dumps(message),
                # properties=pika.BasicProperties(
                #     content_type="application/json",
                #     headers={
                #             'id': header_frame.headers['id'],
                #             'task': header_frame.headers['task']
                #     }
                # ),
            )
            delete_sms_message(message['index'])
            print("Message handled and pushed to queue")
        except Exception:
            print("There was a problem")

try:
    port = serial.Serial(config.serial_port, config.baud_rate, timeout=2)
    if (not port.isOpen()):
        print('Opening Port')
        port.open()

    print('Initializing SIM')
    init(config.sim_card_pin)

    print('Initializing RabbitMQ Connection')
    channel = get_rabbit_connection(
        user = config.RABBIT_USER,
        password = config.RABBIT_PASS,
        server = config.RABBIT_SERVER
    )
    channel.queue_declare(queue=config.RABBIT_QUEUE)

    print("Checking for messages already received")
    collect_and_push_to_rabbit(get_sms_messages())

    # load_phonebook_from_file()
    # send_sms_message("+447xxxxxxxxx", "Testing 123")
    print("Watching serial for inbound messages")
    watch_serial_port()

except KeyboardInterrupt:
    print("Quitting...")
