# Generates a JSON file with randomized account data

import json
import csv
import datetime as dt
import random


def get_creation_date(min_date, max_date, time_format='%Y-%m-%d'):
    start = dt.datetime.strptime(min_date, time_format)
    end = dt.datetime.strptime(max_date, time_format)
    date_diff = abs(start - end).days
    return [dt.datetime.strftime(start + dt.timedelta(days=random.randint(0, date_diff)), time_format),
            dt.datetime.strftime(start + dt.timedelta(days=random.randint(0, date_diff)), time_format),
            dt.datetime.strftime(start + dt.timedelta(days=random.randint(0, date_diff)), time_format)]


def get_subscriber_count(min_count, max_count):
    return [random.randint(min_count, max_count),
            random.randint(min_count, max_count),
            random.randint(min_count, max_count)]


def account_generator(min_date='2019-01-01', max_date='2019-05-30', min_count=100000, max_count=1000000):
    random_creation_date = get_creation_date(min_date, max_date)
    random_sub_count = get_subscriber_count(min_count, max_count)
    acc_dict = {'youtube': [random_creation_date[0], random_sub_count[0]],
                'twitch': [random_creation_date[1], random_sub_count[1]],
                'twitter': [random_creation_date[2], random_sub_count[2]]}
    return acc_dict


with open('streamer_names.csv') as f:
    streamer_names_csv = csv.reader(f)
    account_dict = {}
    for streamer in streamer_names_csv:
        account_dict[streamer[0]] = account_generator()
with open('random_accounts.json', 'w+') as g:
    json.dump(account_dict, g)

