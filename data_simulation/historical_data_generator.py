# Takes a JSON file and writes to Cassandra Cluster
# Fills the day, hour, minute tables according to workflow.txt
# To be run in Cassandra Cluster
# DON'T RUN IN LOCAL

import datetime as dt
import json
import numpy as np
from cassandra.cluster import Cluster, Session


# Writes data to Cassandra Cluster
def write_to_cassandra(session: Session, random_step_data: dict, table: str) -> None:
    session.execute("INSERT INTO insight."
                    + table +
                    " (streamer, timestamp, twitch_count, twitter_count, youtube_count, total_count) VALUES ('"
                    + random_step_data.get('streamer') + "', "
                    + "'" + random_step_data.get('timestamp') + "', "
                    + str(random_step_data.get('twitch_count')) + ", "
                    + str(random_step_data.get('twitter_count')) + ", "
                    + str(random_step_data.get('youtube_count')) + ", "
                    + str(random_step_data.get('total_count')) + ");"
                    )


# We generate bounded random walk with an std of 10% of the max. We assume initial subscriber_count is 0
def random_walk_day_generator(streamer: str, accounts_data: dict, time_format: str, simulations: int,
                              initial_datetime: dt.datetime = dt.datetime.strptime('2019-01-01', '%Y-%m-%d')
                              ) -> list:

    followers_twitter = accounts_data.get('platform_data').get('twitter').get('total_followers')
    followers_twitch = accounts_data.get('platform_data').get('twitch').get('total_followers')
    followers_youtube = accounts_data.get('platform_data').get('youtube').get('total_followers')

    initial_date_twitter = accounts_data.get('platform_data').get('twitter').get('initial_date')
    initial_date_twitch = accounts_data.get('platform_data').get('twitch').get('initial_date')
    initial_date_youtube = accounts_data.get('platform_data').get('youtube').get('initial_date')

    twitter_simulations = get_number_of_simulations(dt.datetime.strptime(initial_date_twitter, time_format))
    twitch_simulations = get_number_of_simulations(dt.datetime.strptime(initial_date_twitch, time_format))
    youtube_simulations = get_number_of_simulations(dt.datetime.strptime(initial_date_youtube, time_format))

    random_steps_twitter = ([0] * (simulations - twitter_simulations)) + \
                           bounded_random_walk(twitter_simulations, 0, followers_twitter, followers_twitter * 0.01)

    random_steps_twitch = ([0] * (simulations - twitch_simulations)) + \
                          bounded_random_walk(twitch_simulations, 0, followers_twitch, followers_twitch * 0.01)

    random_steps_youtube = ([0] * (simulations - youtube_simulations)) + \
                           bounded_random_walk(youtube_simulations, 0, followers_youtube, followers_youtube * 0.01)

    for i, (twitter_step, twitch_step, youtube_step) in enumerate(
            zip(random_steps_twitter, random_steps_twitch, random_steps_youtube)):
        yield {'streamer': streamer,
               'timestamp': dt.datetime.strftime(initial_datetime + dt.timedelta(days=i), time_format),
               'twitch_count': int(twitch_step),
               'twitter_count': int(twitter_step),
               'youtube_count': int(youtube_step),
               'total_count': int(twitch_step) + int(twitter_step) + int(youtube_step)}


def random_walk_hour_generator(streamer: str, accounts_data: dict, time_format: str, simulations: int,
                               initial_datetime: dt.datetime = dt.datetime.today() - dt.timedelta(days=2)
                               ) -> list:

    followers_twitter = accounts_data.get('platform_data').get('twitter').get('total_followers')
    followers_twitch = accounts_data.get('platform_data').get('twitch').get('total_followers')
    followers_youtube = accounts_data.get('platform_data').get('youtube').get('total_followers')

    random_steps_twitter = bounded_random_walk(simulations, 0, followers_twitter, followers_twitter * 0.01)

    random_steps_twitch = bounded_random_walk(simulations, 0, followers_twitch, followers_twitch * 0.01)

    random_steps_youtube = bounded_random_walk(simulations, 0, followers_youtube, followers_youtube * 0.01)

    # Get two days ago at 00:00
    initial_datetime = dt.datetime.strptime(initial_datetime.strftime('%Y-%m-%d') + '_00', time_format)

    for i, (twitter_step, twitch_step, youtube_step) in enumerate(
            zip(random_steps_twitter, random_steps_twitch, random_steps_youtube)):
        yield {'streamer': streamer,
               'timestamp': dt.datetime.strftime(initial_datetime + dt.timedelta(hours=i), time_format),
               'twitch_count': int(twitch_step),
               'twitter_count': int(twitter_step),
               'youtube_count': int(youtube_step),
               'total_count': int(twitch_step) + int(twitter_step) + int(youtube_step)}


def random_walk_minute_generator(streamer: str, accounts_data: dict, time_format: str, simulations: int,
                                 initial_datetime: dt.datetime = dt.datetime.today() - dt.timedelta(hours=2)
                                 ) -> list:

    followers_twitter = accounts_data.get('platform_data').get('twitter').get('total_followers')
    followers_twitch = accounts_data.get('platform_data').get('twitch').get('total_followers')
    followers_youtube = accounts_data.get('platform_data').get('youtube').get('total_followers')

    random_steps_twitter = bounded_random_walk(simulations, 0, followers_twitter, followers_twitter * 0.01)

    random_steps_twitch = bounded_random_walk(simulations, 0, followers_twitch, followers_twitch * 0.01)

    random_steps_youtube = bounded_random_walk(simulations, 0, followers_youtube, followers_youtube * 0.01)

    # Get two days ago at 00:00
    initial_datetime = dt.datetime.strptime(initial_datetime.strftime('%Y-%m-%d_%H') + '-00', time_format)

    for i, (twitter_step, twitch_step, youtube_step) in enumerate(
            zip(random_steps_twitter, random_steps_twitch, random_steps_youtube)):
        yield {'streamer': streamer,
               'timestamp': dt.datetime.strftime(initial_datetime + dt.timedelta(minutes=i), time_format),
               'twitch_count': int(twitch_step),
               'twitter_count': int(twitter_step),
               'youtube_count': int(youtube_step),
               'total_count': int(twitch_step) + int(twitter_step) + int(youtube_step)}


def bounded_random_walk(length, start, end, std, lower_bound=0, upper_bound=np.inf):
    assert (lower_bound <= start and lower_bound <= end)
    assert (start <= upper_bound and end <= upper_bound)

    bounds = upper_bound - lower_bound

    rand = (std * (np.random.random(length) - 0.5)).cumsum()
    rand_trend = np.linspace(rand[0], rand[-1], length)

    rand_deltas = (rand - rand_trend)
    rand_deltas /= np.max([1, (rand_deltas.max()-rand_deltas.min())/bounds])

    trend_line = np.linspace(start, end, length)
    upper_bound_delta = upper_bound - trend_line
    lower_bound_delta = lower_bound - trend_line

    upper_slips_mask = (rand_deltas-upper_bound_delta) >= 0
    upper_deltas = rand_deltas - upper_bound_delta
    rand_deltas[upper_slips_mask] = (upper_bound_delta - upper_deltas)[upper_slips_mask]

    lower_slips_mask = (lower_bound_delta-rand_deltas) >= 0
    lower_deltas = lower_bound_delta - rand_deltas
    rand_deltas[lower_slips_mask] = (lower_bound_delta + lower_deltas)[lower_slips_mask]

    return list(trend_line + rand_deltas)


def write_accounts(session: Session, accounts: dict) -> None:

    games_prep = session.prepare("insert into accounts (streamer, language, game) values (?,?,?)")

    for streamer, attributes in accounts.items():
        session.execute(games_prep, [streamer, attributes['language'], attributes['game']])

    print('ACCOUNTS DONE.')


def write_games(session: Session, games: dict) -> None:

    games_prep = session.prepare("insert into games (game, genre, console) values (?,?,?)")

    for game, attributes in games.items():
        session.execute(games_prep, [game, attributes['genre'], attributes['console']])

    print('GAMES DONE.')


def generate_day_data(session: Session, accounts: dict, simulations: int) -> None:
    time_format = '%Y-%m-%d'
    for streamer, account_data in accounts.items():
        for random_step in random_walk_day_generator(streamer, account_data, time_format, simulations):
            write_to_cassandra(session, random_step, 'unified_day')
    print('DAYS DONE.')


def generate_hour_data(session: Session, accounts: dict, simulations: int) -> None:
    time_format = '%Y-%m-%d_%H'
    for streamer, account_data in accounts.items():
        for random_step in random_walk_hour_generator(streamer, account_data, time_format, simulations):
            write_to_cassandra(session, random_step, 'unified_hour')
    # data = {'streamer': '', date: '', 'twitch_count': 1, 'twitter_count': 1, 'youtube_count': 1, 'total_count': 3}
    print('HOURS DONE.')


def generate_minute_data(session: Session, accounts: dict, simulations: int) -> None:
    time_format = '%Y-%m-%d_%H-%M'
    for streamer, account_data in accounts.items():
        for random_step in random_walk_minute_generator(streamer, account_data, time_format, simulations):
            write_to_cassandra(session, random_step, 'unified_minute')
    # data = {'streamer': '', date: '', 'twitch_count': 1, 'twitter_count': 1, 'youtube_count': 1, 'total_count': 3}
    print('MINUTES DONE.')


def get_accounts(accounts_file: str = 'accounts_info.json') -> dict:
    with open(accounts_file) as acc_file:
        acc_dict = json.load(acc_file)
    return acc_dict


def get_games(games_file: str = 'games_info.json') -> dict:
    with open(games_file) as g_file:
        g_dict = json.load(g_file)
    return g_dict


def get_number_of_simulations(initial_datetime: dt.datetime = dt.datetime.strptime('2019-01-01', '%Y-%m-%d'),
                              final_datetime: dt.datetime = dt.datetime.today() - dt.timedelta(days=2)
                              ) -> int:
    return abs(initial_datetime - final_datetime).days


if __name__ == '__main__':
    cassandra_cluster = Cluster(['10.0.0.5', '10.0.0.7', '10.0.0.12', '10.0.0.19'])
    cassandra_session = cassandra_cluster.connect('insight')

    number_of_simulations = get_number_of_simulations()

    accounts_dict = get_accounts()
    games_dict = get_games()

    write_accounts(cassandra_session, accounts_dict)
    write_games(cassandra_session, games_dict)

    # TimeZones can mess with this part, not too relevant for a simulation but it will give funky results

    generate_day_data(cassandra_session, accounts_dict, number_of_simulations)
    generate_hour_data(cassandra_session, accounts_dict, 46 + dt.datetime.now().hour)
    generate_minute_data(cassandra_session, accounts_dict, 120)

    print('SIMULATION OVER')

    cassandra_cluster.shutdown()
