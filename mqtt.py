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
client_id2 = os.getenv('CLIENT_ID2')
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')

timeIn = time.time()
fileNameTemp = ''
path1 = ''
my_file1 = ''
flagWrite = False

def file_print(data_string, data_data):
    """ Обработчик данных от сервера MQTT. Передаем: топик, данные """
    fileName = data_string[1 : data_string.rfind("/")]
    
    global timeIn, fileNameTemp, path1, my_file1, folder_archive_name, flagWrite
    if ((time.time() - timeIn) > 3):
            fileNameTemp = ''
            timeIn = time.time()
            flagWrite = True

    if (fileNameTemp != fileName):
        if (bool(path1 != '') & flagWrite):
            fileWrite(folder_archive_name, path1, my_file1)
            flagWrite = False
        
        timeIn = time.time()
        my_file1 = ''

        fileNameTemp = fileName
        path1 = f"{fileNameTemp}.csv"
        my_file1 += f"{time.strftime('/%Y/%m/%d - %H:%M:%S', time.localtime())} * {data_data}"

    else:   
        my_file1 += f",{data_data}"


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

            file_print(msg.topic, msg.payload.decode())
        except Exception as error:
            print(error)

    client.subscribe(topic)
    client.on_message = on_message


def fileWrite(folderArhiveName, path, myFile):
    """ Запись в файл. Передаем: название папки архива, название файла (топика), данные """
    file_name_path = folderArhiveName + myFile[:myFile.find(' ')]

    pathTemp = f"{Path.cwd()}/{file_name_path}"

    if not Path(pathTemp).exists():
        Path(pathTemp).mkdir(parents=True, exist_ok=True)

    my_file = open(f"{pathTemp}/{path}", "a")
    my_file.write(myFile + '\n')
    my_file.close() 
    print('\n---------------')
    print(myFile)
    print('---------------')
    print('my_File= ', path)


def run():
    try:
        client = connect_mqtt()
        subscribe(client)
        client.loop_forever()
    except KeyboardInterrupt:
        global path1, folder_archive_name, my_file1
        if (path1 != ''):
            fileWrite(folder_archive_name, path1, my_file1)

        print("\nBye bye !")
        return 0


if __name__ == "__main__":
    run()
