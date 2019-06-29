# Generates a JSON file with randomized data for the accounts table
# The JSON file is fed into the cassandra table generators.

import json
import csv
import datetime as dt
import random
# from cassandra.cluster import Cluster


def get_creation_date(min_date, max_date, time_format='%Y-%m-%d'):
    start = dt.datetime.strptime(min_date, time_format)
    end = dt.datetime.strptime(max_date, time_format)
    date_diff = abs(start - end).days
    return [dt.datetime.strftime(start + dt.timedelta(days=random.randint(0, date_diff)), time_format),
            dt.datetime.strftime(start + dt.timedelta(days=random.randint(0, date_diff)), time_format),
            dt.datetime.strftime(start + dt.timedelta(days=random.randint(0, date_diff)), time_format)]


def get_subscriber_count(min_count, max_count):
    return [random.randint(min_count[0], max_count[0]),
            random.randint(min_count[1], max_count[1]),
            random.randint(min_count[2], max_count[2])]


def account_generator(min_count, max_count, min_date='2019-01-01', max_date='2019-05-30'):
    random_creation_date = get_creation_date(min_date, max_date)
    random_sub_count = get_subscriber_count(min_count, max_count)
    acc_dict = {'youtube': [random_creation_date[0], random_sub_count[0]],
                'twitch': [random_creation_date[1], random_sub_count[1]],
                'twitter': [random_creation_date[2], random_sub_count[2]],
                'game': random.choice(games),
                'language': random.choice(languages),
                }
    return acc_dict


# [youtube, twitch, twitter], minimum and maximum subscriber count
min_count_list = [100e3, 100e3, 10e3]
max_count_list = [100e6, 15e6, 20e6]

languages = ['English', 'Spanish', 'Russian', 'Chinese', 'Korean', 'Japanese', 'Portuguese']

with open('games_info.json') as g:
    games = json.load(g)
    games = list(games.keys())

with open('streamer_names.csv') as f:
    streamer_names_csv = csv.reader(f)
    account_dict = {}
    for streamer in streamer_names_csv:
        account_dict[streamer[0]] = account_generator(min_count_list, max_count_list)
with open('random_accounts.json', 'w+') as g:
    json.dump(account_dict, g)
