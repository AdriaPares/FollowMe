#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun  9 19:23:45 2019

@author: luzjubierrezapater
"""
    
import json
import csv
import pycurl

with open('api_keys.json') as api_keys_json:
    api_keys = json.load(api_keys_json)

#
# with open('./twitch_api/Twitch_API_Client_ID.txt','r') as file:
#     Twitch_API_Client_ID = file.read()[:-1]#last character is /n, not needed.
    
with open('./streamer_names.csv', 'r') as file:
    #reader gives list of lists: [['a'],['b']], hence comprehension to make things easy
    streamer_names = [streamer[0] for streamer in list(csv.reader(file))]
    #maybe not needed, but this is a very rough first draft

   

for name in streamer_names:
    with open('./streamer_info/channel_'+name+'.txt', 'wb+') as f: 
        c = pycurl.Curl()
        c.setopt(c.URL, 'https://api.twitch.tv/api/channels/'+name+'/panels' ) 
        header = ['Client-ID: '+api_keys['twitch']]
        c.setopt(pycurl.HTTPHEADER, header )
        c.setopt(c.WRITEFUNCTION, f.write)
        c.perform()

#with open('test_curl_append.txt', 'r') as f: 
 #   js = json.load(f)

#for name in streamer_names[0:10]: #testing for now, ideally it appends to database
    #curl -H 'Client-ID: fm797nnar5v3m2phub2d54i9usabai' -X GET 'https://api.twitch.tv/api/channels/nightblue3/panels'
    #deprecated API v5
 #   print(name)
    
    #test the time it takes to load all 500