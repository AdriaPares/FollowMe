#!/bin/bash
export SPARK_HOME=/usr/local/spark
python3 ~/cassandra_jobs/youtube_hour_to_day.py
python3 ~/cassandra_jobs/twitch_hour_to_day.py
python3 ~/cassandra_jobs/twitter_hour_to_day.py