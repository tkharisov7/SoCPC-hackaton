import threading
import time
from pyspark.sql.types import StringType
import json
import ast
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic


def get_event(string, event):
    """gets current event"""
    if string == {}:
        return None
    dic = ast.literal_eval(string)
    if event in dic.keys():
        return dic[event]
    else:
        return None


def compare_time(left, right):
    """compares two time string"""
    if left is None:
        return False
    if right is None:
        return True
    return left < right


def is_another_day(time_web: str, time_mobile: str) -> bool:
    """checks if days are different"""
    if time_web is None:
        return True
    if time_web < time_mobile:
        time_web, time_mobile = time_mobile, time_web
    year1 = time_web[0:4]
    year2 = time_mobile[0:4]
    if year1 != year2:
        return True
    month1 = time_web[5:7]
    month2 = time_mobile[5:7]
    if month1 != month2:
        return True
    day1 = time_web[8:10]
    day2 = time_mobile[8:10]
    if day1 != day2:
        return True
    return False


def from_date_to_mobile_consumers(date: str) -> float:
    """turns date into time"""
    hours_f = float(date[11:13])
    minutes_f = float(date[14:16])
    mobile_consumers_f = float(date[17:19])
    mmobile_consumers_f = float(date[20:23]) / 1000
    return mmobile_consumers_f + mobile_consumers_f + minutes_f * 60 + hours_f * 3600


def is_diff_more_than_5(date1: str, date2: str) -> bool:
    """checks if time differs ar least in five minutes"""
    mobile_consumers1 = from_date_to_mobile_consumers(date1)
    mobile_consumers2 = from_date_to_mobile_consumers(date2)
    if mobile_consumers1 < mobile_consumers2:
        mobile_consumers1, mobile_consumers2 = mobile_consumers2, mobile_consumers1
    if mobile_consumers1 - mobile_consumers2 > 300:
        return True
    return False


def plus_five_minutes(date: str) -> str:
    """adds five minutes"""
    prev_minutes = int(date[14:16])
    prev_hours = int(date[11:13])
    prev_minutes += prev_hours * 60 + 5
    new_minutes = prev_minutes % 60
    new_hours = prev_minutes // 60
    if new_hours * 60 + new_minutes < 5:
        return date[:11] + "23:59:59.999" + date[23:]
    str_hours = "0" + str(new_hours)
    str_minutes = "0" + str(new_minutes)
    return date[:11] + str_hours[-2:] + ":" + str_minutes[-2:] + date[16:]


def kafka(month_data_dict):
    """read dict and put final topics"""
    try:
        admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
        topic = NewTopic(name='out_cm',
                         num_partitions=1,
                         replication_factor=1)
        admin.create_topics([topic])
    except Exception:
        pass

    consumer_mobile = KafkaConsumer(bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest',
                             group_id='my-group-mobile')
    consumer_mobile.subscribe(['mobile_client'])
    consumer_web = KafkaConsumer(bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest',
                               group_id='my-group-web')
    consumer_web.subscribe(['web_client'])
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    my_set = set([])
    web_set = set([])
    last_five_min_list = []
    read_web_consumer = True
    read_mobile_consumer = True
    time_web = None
    time_mobile = None
    id_web = None
    id_mobile = None
    web_consumer = {}
    mobile_consumer = {}
    current_day = {}
    while True:
        #reads new info only if it is necessary
        if read_web_consumer:
            web_consumer = consumer_mobile.poll(1, 1)
            for it in web_consumer.values():
                web_consumer = it[0].value.decode('utf-8')
            time_web = get_event(web_consumer, 'eventTime')
            id_web = get_event(web_consumer, 'id')
            read_web_consumer = False

        if read_mobile_consumer:
            mobile_consumer = consumer_web.poll(1, 1)
            for it in mobile_consumer.values():
                mobile_consumer = it[0].value.decode('utf-8')
            time_mobile = get_event(mobile_consumer, 'eventTime')
            id_mobile = get_event(mobile_consumer, 'id')
            read_mobile_consumer = False

        if time_web is None:
            read_web_consumer = True
        if time_mobile is None:
            read_mobile_consumer = True

        if time_web is not None or time_mobile is not None:
            if compare_time(time_web, time_mobile):

                if len(last_five_min_list) != 0:
                    while is_diff_more_than_5(get_event(web_consumer, 'eventTime'), last_five_min_list[0]['eventTime']):
                        current = last_five_min_list.pop(0)
                        if current['client_id'] not in my_set:
                            current["eventTime"] = plus_five_minutes(current["eventTime"])
                            current["data_all_mb"] = month_data_dict[current["id"]]["data_all_mb"]
                            current["voice_out_sec"] = month_data_dict[current["id"]]["voice_out_sec"]
                            current["voice_in_sec"] = month_data_dict[current["id"]]["voice_in_sec"]
                            producer.send('out_cm', value=current)

                        if len(last_five_min_list) == 0:
                            break

                if id_web not in my_set:
                    my_set.add(id_web)
                    current_info = ast.literal_eval(web_consumer)
                    correct_info = {"client_id": current_info["id"],
                                    "eventTime": current_info["eventTime"],
                                    "channel": "push"}
                    producer.send('out_cm', value=correct_info)
                    with open('data/out_cm.json', 'w') as file:
                        correct_info["data_all_mb"] = month_data_dict[current_info["id"]]["data_all_mb"]
                        correct_info["voice_out_sec"] = month_data_dict[current_info["id"]]["voice_out_sec"]
                        correct_info["voice_in_sec"] = month_data_dict[current_info["id"]]["voice_in_sec"]
                        json.dump(str(correct_info), file)


                print(web_consumer)
                read_web_consumer = True
                if is_another_day(get_event(current_day, 'eventTime'), time_web):
                    current_day = web_consumer
                    while len(last_five_min_list) != 0:
                        current = last_five_min_list.pop(0)
                        if current['client_id'] not in my_set:
                            my_set.add(current['client_id'])
                            current['eventTime'] = ast.literal_eval(current_day)['eventTime']
                            producer.send('out_cm', value=current)
                            with open('data/out_cm.json', 'w') as file:
                                current["data_all_mb"] = month_data_dict[current["id"]]["data_all_mb"]
                                current["voice_out_sec"] = month_data_dict[current["id"]]["voice_out_sec"]
                                current["voice_in_sec"] = month_data_dict[current["id"]]["voice_in_sec"]
                                json.dump(str(current), file)

                    my_set = set([])
                    web_set = set([])
                    last_five_min_list = []

            else:
                if len(last_five_min_list) != 0:
                    while is_diff_more_than_5(get_event(mobile_consumer, 'eventTime'), last_five_min_list[0]['eventTime']):
                        current = last_five_min_list.pop(0)
                        if current['client_id'] not in my_set:
                            current['eventTime'] = plus_five_minutes(current['eventTime'])
                            producer.send('out_cm', value=current)
                            with open('data/out_cm.json', 'w') as file:
                                current["data_all_mb"] = month_data_dict[current["id"]]["data_all_mb"]
                                current["voice_out_sec"] = month_data_dict[current["id"]]["voice_out_sec"]
                                current["voice_in_sec"] = month_data_dict[current["id"]]["voice_in_sec"]
                                json.dump(str(current), file)

                        if len(last_five_min_list) == 0:
                            break

                if id_mobile not in my_set:
                    if id_mobile not in web_set:
                        current_info = ast.literal_eval(mobile_consumer)
                        correct_info = {"client_id": current_info["id"],
                                        "eventTime": current_info["eventTime"],
                                        "channel": "sms"}
                        last_five_min_list.append(correct_info)
                    web_set.add(id_mobile)
                print(mobile_consumer)
                read_mobile_consumer = True
                if is_another_day(get_event(current_day, 'eventTime'), time_mobile):
                    current_day = mobile_consumer
                    while len(last_five_min_list) != 0:
                        current = last_five_min_list.pop(0)
                        if current['client_id'] not in my_set:
                            my_set.add(current['client_id'])
                            current['eventTime'] = ast.literal_eval(current_day)['eventTime']
                            producer.send('out_cm', value=current)
                            with open('data/out_cm.json', 'w') as file:
                                current["data_all_mb"] = month_data_dict[current["id"]]["data_all_mb"]
                                current["voice_out_sec"] = month_data_dict[current["id"]]["voice_out_sec"]
                                current["voice_in_sec"] = month_data_dict[current["id"]]["voice_in_sec"]
                                json.dump(str(current), file)

                    my_set = set([])
                    web_set = set([])
                    last_five_min_list = []
