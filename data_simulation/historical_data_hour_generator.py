# Takes a JSON file and writes to Cassandra Cluster
# Fills the hour tables according to workflow.txt
# To be run in Cassandra Cluster
# DON'T RUN IN LOCAL
# BackUp copy

import datetime as dt
# import time
import json
import numpy as np
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement


def generate_data(streamer, data, final_date=dt.datetime.today(), time_format='%Y-%m-%d_%H'):
    for website, (creation_date, current_subscriber_count) in data.items():
        creation_datetime = dt.datetime.today() - dt.timedelta(days=2)
        try:
            final_datetime = dt.datetime.strptime(final_date, time_format)
        except TypeError:
            final_datetime = final_date
        number_of_simulations = int(abs(creation_datetime - final_datetime).total_seconds()//3600)

        for timestamp, simulated_subscriber_count in random_walk_generator(creation_datetime, number_of_simulations,
                                                                           current_subscriber_count):
            write_to_cassandra(timestamp, website, streamer, simulated_subscriber_count)


def write_to_cassandra(timestamp, website, streamer, current_subscriber_count):
    if website == 'twitch':
        cassandra_session.execute(twitch_prep, [streamer, timestamp, current_subscriber_count])
    elif website == 'twitter':
        cassandra_session.execute(twitter_prep, [streamer, timestamp, current_subscriber_count])
    elif website == 'youtube':
        cassandra_session.execute(youtube_prep, [streamer, timestamp, current_subscriber_count])


def random_walk_generator(creation_datetime, number_of_simulations, current_subscriber_count,
                          time_format='%Y-%m-%d_%H'):
    # we generate bounded random walk with an std of 1% of the max. We assume initial subscriber_count is 0
    random_steps = bounded_random_walk(number_of_simulations, 0, current_subscriber_count,
                                       current_subscriber_count * 0.01)
    for i, simulated_subscriber_count in enumerate(random_steps):
        yield (dt.datetime.strftime(creation_datetime + dt.timedelta(hours=i), time_format),
               int(simulated_subscriber_count))


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

    return trend_line + rand_deltas


cassandra_cluster = Cluster(['10.0.0.5', '10.0.0.7', '10.0.0.12', '10.0.0.19'])
cassandra_session = cassandra_cluster.connect('insight')

twitch_prep = cassandra_session.prepare("insert into twitch_hour (streamer, timestamp, follower_count) values (?,?,?)");
twitter_prep = cassandra_session.prepare("insert into twitter_hour (streamer, timestamp, follower_count) values (?,?,?)");
youtube_prep = cassandra_session.prepare("insert into youtube_hour (streamer, timestamp, follower_count) values (?,?,?)");

with open('random_accounts.json') as f:
    accounts_dict = json.load(f)
for streamer_name, streamer_data in accounts_dict.items():
    generate_data(streamer_name, streamer_data, final_date=dt.datetime.today() - dt.timedelta(hours=2))
print('HOURS DONE.')

cassandra_cluster.shutdown()
