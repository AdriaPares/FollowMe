#!/bin/bash
export SPARK_HOME=/usr/local/spark
python3 ~/cassandra_jobs/youtube_live_to_minute.py
python3 ~/cassandra_jobs/twitch_live_to_minute.py
python3 ~/cassandra_jobs/twitter_live_to_minute.py