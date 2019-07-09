import boto3
import json
import glob
# import numpy as np
import time
import datetime as dt

# This needs to run on S3


def number_of_simulations(json_file_names):
    timestamps = []
    for name in json_file_names:
        timestamps.append(dt.datetime.strptime(name[:19], '%Y-%m-%d_%H:%M:%S'))
    # print(int(abs(timestamps[0] - timestamps[1]).total_seconds()))
    # time.sleep(50)
    return int(abs(timestamps[0] - timestamps[1]).total_seconds())


def get_simulation_timestamps(oldest_file_name, sim_number, time_format='%Y-%m-%d_%H:%M:%S'):
    # file name is the oldest timestamp
    for i in range(sim_number):
        yield (i, (dt.datetime.strptime(oldest_file_name[:19], time_format) + dt.timedelta(seconds=i + 1))
               .strftime(time_format) + oldest_file_name[19:])


def write_simulation(oldest_file_name, sim_number, sim_step, oldest_sub_count):
    with open(oldest_file_name) as old_file:
        json_template = json.load(old_file)
    for counter, file_name in get_simulation_timestamps(oldest_file_name, sim_number):
        json_template['total'] = oldest_sub_count + sim_step * counter
        with open('./twitch_sim_dump/' + file_name, 'w+') as f:
            json.dump(json_template, f)
            print('done')
            time.sleep(1)
    pass


def simulate_youtube():
    pass


def simulate_twitch(json_file_names):  # Make this call a padded call: def fun(*args)
    # json_file_1 has the oldest timestamps (comes before in time than 2)
    simulation_number = number_of_simulations(json_file_names)
    subscriber_counts = []
    for json_file in sorted(json_file_names):  # subs[0] has the oldest sub number
        # print(json_file)
        with open(json_file) as f:
            files = json.load(f)
            subscriber_counts.append(files.get('total', ''))
    simulation_subs_step = int(abs(subscriber_counts[0] - subscriber_counts[1]) / simulation_number)
    write_simulation(sorted(json_file_names)[0], simulation_number, simulation_subs_step, subscriber_counts[0])
    # simulation_subs_step = int(abs(subs[0]-subs[1])/simulation_number)
    # for sim_name in get_simulation_timestamps(sorted(json_file_names)[0], simulation_number):
    #
    #     write_simulation(sim_name, sorted(json_file_names)[0])


def simulate_twitter():
    pass


# this needs to read from accounts.json
streamer_names = ['playbattlegrounds', 'lol']


# this is very inefficient, surely there's a better way to do this
for streamer_name in streamer_names:
    # print(streamer_name)
    simulate_twitch(glob.glob('*_twitch_'+streamer_name+'.json'))  # make it an unpacking pass *glob....
