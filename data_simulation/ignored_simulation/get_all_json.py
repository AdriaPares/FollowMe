#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 11 09:54:49 2019

@author: luzjubierrezapater
"""

# This script needs to have the streamer accounts already in JSON files
# Add try_excepts all over the place
# Add decorators to functions

# THIS SCRIPT ONLY HAS TO GET AND DUMP THE JSON FILES. NO PROCESSING!!

# There's a ton of problems with the names, for now just go!

import json
import glob
import tweepy
import pycurl
import urllib.request
import time


def get_twitch_data(accounts, api_key, path="./api_dumps/twitch_dump/"):

    # https: // api.twitch.tv / helix / users?login = < username >  GETS ID FROM USERNAME

    # https: // api.twitch.tv / helix / users / follows?to_id = < user ID >  GETS FOLLOWERS FROM ID
    with open(path + 'twitch_' + accounts['player'] + '.json', 'wb+') as f:
        c = pycurl.Curl()
        c.setopt(c.URL, 'https://api.twitch.tv/helix/users/follows?to_id=' + str(accounts['twitch']))
        header = ['Client-ID: ' + api_key['twitch']]
        c.setopt(pycurl.HTTPHEADER, header)
        c.setopt(c.WRITEFUNCTION, f.write)
        c.perform()
    time.sleep(5)


def get_twitter_data(accounts, api_key, path="./api_dumps/twitter_dump/"):
    auth = tweepy.OAuthHandler(api_key['twitter']['Consumer API Key'], api_key['twitter']['Consumer API Secret Key'])
    auth.set_access_token(api_key['twitter']['Access token'], api_key['twitter']['Access token secret'])
    api = tweepy.API(auth, wait_on_rate_limit=True)
    try:
        # print(accounts['twitter'].split('twitter.com/')[-1])
        user = api.get_user(accounts['twitter'].split('twitter.com/')[-1])
        # print(user)
        # followers = user.followers_count
        # print(followers)
        # Dump the results
        with(open(path + 'twitter_' + accounts['player']+'.json', 'w+')) as f:
            json.dump(user._json, f)
    except tweepy.error.TweepError:
        # print('No twitter account')
        get_missing_accounts(accounts, 'twitter')
    pass


def get_youtube_data(accounts, api_key, path="./api_dumps/youtube_dump/"):
    # We can have the data from the user or from the id...
    if accounts['youtube'][0] == 'user_name' or accounts['youtube'][0] == 'c' or accounts['youtube'][0] == 'user':
        data = urllib.request.urlopen("https://www.googleapis.com/youtube/v3/channels?part=statistics&forUsername="
                                      + accounts['youtube'][1].split('youtube.com/')[-1]
                                      + "&key=" + api_key['youtube'].split('youtube.com/')[-1]).read()
    elif accounts['youtube'][0] == 'channel':
        data = urllib.request.urlopen("https://www.googleapis.com/youtube/v3/channels?part=statistics&id="
                                      + accounts['youtube'][1].split('youtube.com/')[-1]
                                      + "&key=" + api_key['youtube'].split('youtube.com/')[-1]).read()
    else:
        data = ''
    # subs = json.loads(data)['items']
    with open(path + 'youtube_' + accounts['player'] + '.json', 'w+') as f:
        if data != '':
            json.dump(json.loads(data.decode('utf-8')), f)

    # print(subs)


def get_missing_accounts(accounts, api = None):
    pass


init = time.time()
json_path_accounts = './streamer_accounts/accounts_*.json'
json_file_names = glob.glob(json_path_accounts)
with open('api_keys.json') as api_keys_json:
    api_keys = json.load(api_keys_json)
with open('accounts.json') as accounts_json:
    streamer_accounts = json.load(accounts_json)
    for streamer in streamer_accounts:
        get_twitch_data(streamer, api_keys)
        get_twitter_data(streamer, api_keys)
        get_youtube_data(streamer, api_keys)
        print('hey')
print(time.time() - init)
