import boto3
import json
import glob
import numpy as np
import time
import datetime


def number_of_simulations(json_file_names):
    hhmmss_timestamps = []
    for name in json_file_names:
        hhmmss_timestamps.append(name.split('_')[1].split(':'))
    # there has to be a better way to do this for sure
    hhmmss_array = np.array([int(x) - int(y) for x, y in zip(hhmmss_timestamps[0], hhmmss_timestamps[1])])
    return np.abs(np.sum(np.array([3600, 60, 1]) * hhmmss_array))


def get_simulation_timestamps(json_file_name, simulation_number):
    li = [time.strftime("%Y-%m-%d_%H:%M:%S_")+]

    print(json_file_name)
    print(simulation_number)
    return []


def simulate_youtube():
    pass


def simulate_twitch(json_file_names):
    simulation_number = number_of_simulations(json_file_names)
    subs = []
    for json_file in json_file_names:
        # print(json_file)
        with open(json_file) as f:
            files = json.load(f)
            subs.append(files.get('total', ''))

    datetime.strptime('', '%b %d %Y %I:%M%p')

    simulation_subs_step = int(abs(subs[0]-subs[1])/simulation_number)
    simulation_timestamps = get_simulation_timestamps(json_file, simulation_number)
    time.sleep(50)
    for simulation in range(simulation_number):
        files['total'] = subs[1]-simulation_subs_step
        with open(json_file, 'w+') as f:
            json.dump(files, f)
        time.sleep(10)


def simulate_twitter():
    pass


# this needs to read from accounts.json
streamer_names = ['playbattlegrounds', 'lol']


# this is very inefficient, surely there's a better way to do this
for streamer_name in streamer_names:
    # print(streamer_name)
    simulate_twitch(glob.glob('*_twitch_'+streamer_name+'.json'))
