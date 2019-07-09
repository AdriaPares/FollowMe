import json
import glob
import datetime as dt
import time
import boto3


def batch_dummy_simulate_twitch(json_file_names):
    simulation_number = 86400
    for json_file in sorted(json_file_names):  # subs[0] has the oldest sub number
        # print(json_file)
        with open(json_file) as f:
            files = json.load(f)
            subscriber_count = files.get('total', '')
    simulation_subs_step = 1
    write_simulation(sorted(json_file_names)[0], simulation_number, simulation_subs_step, subscriber_count)
    time.sleep(50)


def get_simulation_timestamps(oldest_file_name, sim_number=86400, time_format='%Y-%m-%d_%H-%M-%S'):
    # file name is the oldest timestamp
    # 216000 simulates a full day
    for i in range(sim_number):
        yield (i, (dt.datetime.strptime(oldest_file_name[:19], time_format) + dt.timedelta(seconds=i + 1))
               .strftime(time_format) + oldest_file_name[19:])


def write_simulation(oldest_file_name, sim_number, sim_step, oldest_sub_count):
    with open(oldest_file_name) as old_file:
        json_template = json.load(old_file)
    s3 = boto3.resource('s3')
    for counter, file_name in get_simulation_timestamps(oldest_file_name, sim_number):
        json_template['total'] = oldest_sub_count + sim_step * counter
        with open('dummy_batch_dump/twitch_dummy/'+file_name, 'w+') as f:
            json.dump(json_template, f)
        s3.Object('insight-api-dumps', 'dummy_batch_dump/twitch_dummy/'+file_name)\
            .upload_file(Filename='dummy_batch_dump/twitch_dummy/'+file_name)
        print('yo')
        time.sleep(100)


streamer_names = ['testing']
for streamer_name in streamer_names:
    # print(streamer_name)
    batch_dummy_simulate_twitch(glob.glob('*_twitch_'+streamer_name+'.json'))  # make it an unpacking pass *glob....
