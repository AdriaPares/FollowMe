#!/bin/bash
python3 ./consumers/youtube_consumer.py &
python3 ./consumers/twitter_consumer.py &
python3 ./producers/twitch_producer.py &