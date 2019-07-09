#!/bin/bash
python3 ./consumers/twitch_consumer.py &
python3 ./consumers/twitter_consumer.py &
python3 ./producers/youtube_producer.py &