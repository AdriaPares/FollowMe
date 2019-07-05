# Generates a JSON file with randomized data for the accounts table
# The JSON file is fed into the Cassandra table generators.

import json
import csv
import datetime as dt
import random


def get_creation_date(min_date: str, max_date: str, time_format: str = '%Y-%m-%d') -> list:
    start = dt.datetime.strptime(min_date, time_format)
    end = dt.datetime.strptime(max_date, time_format)
    date_diff = abs(start - end).days
    return [dt.datetime.strftime(start + dt.timedelta(days=random.randint(0, date_diff)), time_format),
            dt.datetime.strftime(start + dt.timedelta(days=random.randint(0, date_diff)), time_format),
            dt.datetime.strftime(start + dt.timedelta(days=random.randint(0, date_diff)), time_format)]


def get_subscriber_count(min_count: list, max_count: list) -> list:
    return [random.randint(min_count[0], max_count[0]),
            random.randint(min_count[1], max_count[1]),
            random.randint(min_count[2], max_count[2])]


def account_generator(games: list, languages: list, min_count: list, max_count: list,
                      min_creation_date: str = '2019-01-01', max_creation_date: str = '2019-05-30') -> dict:
    random_creation_date = get_creation_date(min_creation_date, max_creation_date)
    random_sub_count = get_subscriber_count(min_count, max_count)
    acc_dict = {'platform_data': {
        'youtube': {'initial_date': random_creation_date[0], 'total_followers': random_sub_count[0]},
        'twitch': {'initial_date': random_creation_date[1], 'total_followers': random_sub_count[1]},
        'twitter': {'initial_date': random_creation_date[2], 'total_followers': random_sub_count[2]}
    },
        'game': random.choice(games),
        'language': random.choice(languages)
    }
    return acc_dict


def old_account_generator(games: list, languages: list, min_count: list, max_count: list,
                          min_creation_date: str = '2019-01-01', max_creation_date: str = '2019-05-30') -> dict:
    random_creation_date = get_creation_date(min_creation_date, max_creation_date)
    random_sub_count = get_subscriber_count(min_count, max_count)
    acc_dict = {'platform_data': {
        'youtube': [random_creation_date[0], random_sub_count[0]],
        'twitch': [random_creation_date[1], random_sub_count[1]],
        'twitter': [random_creation_date[2], random_sub_count[2]]
    },
        'game': random.choice(games),
        'language': random.choice(languages)
    }
    return acc_dict


def get_min_and_max_follower_count() -> (list, list):
    # [youtube, twitch, twitter], minimum and maximum subscriber count
    min_count = [100e3, 100e3, 10e3]
    max_count = [100e6, 15e6, 20e6]
    return min_count, max_count


def get_languages() -> list:

    return ['English', 'Spanish', 'Russian', 'Chinese', 'Korean', 'Japanese', 'Portuguese']


def get_games(games_file: str = 'games_info.json') -> list:
    with open(games_file) as g:
        game_dict = json.load(g)
        games_list = list(game_dict.keys())
    return games_list


def get_accounts(games: list, languages: list, min_count: list, max_count: list,
                 streamer_names_file: str = 'streamer_names.csv') -> (dict, dict):
    with open(streamer_names_file) as f:
        streamer_names_csv = csv.reader(f)
        account_dict = {}
        old_account_dict = {}
        for streamer in streamer_names_csv:
            account_dict[streamer[0]] = account_generator(games, languages, min_count, max_count)
            old_account_dict[streamer[0]] = old_account_generator(games, languages, min_count, max_count)
    return account_dict, old_account_dict


def write_accounts(accounts: dict, accounts_file: str = 'accounts_info.json') -> None:
    with open(accounts_file, 'w+') as g:
        json.dump(accounts, g)


if __name__ == '__main__':

    min_count_list, max_count_list = get_min_and_max_follower_count()
    games_list = get_games()
    languages_list = get_languages()

    accounts_dict, old_accounts_dict = get_accounts(games_list, languages_list, min_count_list, max_count_list)
    write_accounts(accounts_dict, 'games_info.json')
    write_accounts(old_accounts_dict, 'random_accounts.json')

