#!/bin/bash
python3 ./consumers/twitch_consumer.py &
python3 ./consumers/youtube_consumer.py &
python3 ./producers/twitter_producer.py &