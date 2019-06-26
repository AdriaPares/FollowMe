#!/bin/bash
export SPARK_HOME=/usr/local/spark
python3 ~/cassandra_jobs/youtube_minute_to_hour.py
python3 ~/cassandra_jobs/twitch_minute_to_hour.py
python3 ~/cassandra_jobs/twitter_minute_to_hour.py