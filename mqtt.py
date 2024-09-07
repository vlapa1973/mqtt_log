#!/usr/bin/env python3

import os
import paho.mqtt.client as mqtt
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

folder_archive_name = str("data")


broker = os.getenv('BROKER')
port = int(os.getenv('PORT'))
topic = os.getenv('TOPIC')
topic2 = os.getenv('TOPIC2')
client_id2 = os.getenv('CLIENT_ID2')
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')


def file_print(data_string, data_data):

    file_name = data_string[: data_string.rfind("/")]
    file_name_tmp = data_string[data_string.rfind("/") :]
    file_name_path = folder_archive_name + time.strftime("/%Y/%m/%d/", time.localtime())

    path = f"{Path.cwd()}/{file_name_path}/{file_name}"

    if not Path(path).exists():
        Path(path).mkdir(parents=True, exist_ok=True)

    path = f"{path}{file_name_tmp}.csv"

    my_file = open(path, "a")
    my_file.write(f"{time.strftime('%H:%M:%S', time.localtime())},{data_data}\n")
    my_file.close()


def connect_mqtt():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    client.username_pw_set(username, password)
    client.connect(broker, port)
    return client


def subscribe(client: mqtt):

    def on_message(client, userdata, msg):
        try:
            topicNew: str = ""
            if len(msg.topic) < 24:
                topicNew = msg.topic + "\t\t"
            elif len(msg.topic) < 32:
                topicNew = msg.topic + "\t"

            print(
                f"{time.strftime('%H:%M:%S', time.localtime())}:\t{topicNew}:\t{msg.payload.decode()}"
            )
            file_print(msg.topic, msg.payload.decode())
        except Exception as error:
            print(error)

    client.subscribe(topic)
    client.on_message = on_message


def run():
    try:
        client = connect_mqtt()
        countMy = subscribe(client)
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nBye bye !")
        return 0


if __name__ == "__main__":
    run()
