import os
from dotenv import load_dotenv
import psycopg2
import time
from datetime import datetime
from datetime import timezone

utc_tz = timezone.utc
load_dotenv()


def start_parser():
    connection = psycopg2.connect(user=os.getenv('USER_DB'),
                                       password=os.getenv('PASSWORD_DB'),
                                       dbname=os.getenv('NAME_DB'),
                                       host=os.getenv('HOST_DB'),
                                       port=os.getenv('PORT_DB'))
    connection.autocommit = True  # Включаем автокомит
    cursor = connection.cursor()


    cursor.execute(
        "select state from parser_history where id = 1"
    )

    status = cursor.fetchone()[0]
    print(status)

    if status != 'finished':
        return

    if datetime.now().hour < 2:
        return

    print('start')

    cursor.execute(
        "update parser_history_authors set state = 'wait' where parser_history_id = 1"
    )

    cursor.execute(
        f"update parser_history set state = 'running', stop_time = null,"
        f" start_time = '{datetime.now(utc_tz)}', reels_added = 0, reels_total = 0,"
        f"profiles_processed = 0 where id = 1"
    )




while True:
    start_parser()
    time.sleep(60*10)
