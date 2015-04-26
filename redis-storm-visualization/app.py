from flask import Flask, Response, render_template
# from rediscluster import RedisCluster
import redis as r
import aerospike
import time
import os
import json
import time
from aerospike import predicates as p

app = Flask(__name__)

AEROSPIKE_NAMESPACE = "test"
AEROSPIKE_SET = "stormset-hashtag"
TWEET_HASHTAG_BIN = "hashTag"
TWEET_COUNT = "hashTagCount"
AEROSPIKE_STORMSET = "stormset"
TWEET_ID_BIN = "tweetId_bin"
TWEET_DATETIME_BIN = "tweetDateTime"
TWEET_LOCATION_BIN = "tweetLocation"
TWEET_TEXT_BIN = "tweetText"
USER_NAME_BIN = "userName"

# Aerospike
config = {
    'hosts': [
        ('10.0.0.29', 3000)
    ],
    'policies': {
        'timeout': 1000
    },
    'lua': {
        'user_path': os.path.dirname('udf/toptweets.lua')
    }
}
client = aerospike.client(config).connect()

# Redis
redis = r.StrictRedis(host='10.0.0.29', port=6379, db=0)


def trending_hash_tags_redis():
    trending_hashtags_data = redis.zrange("trending-topics", -20, -1)
    trending_hashtags = ""
    for hashtag in trending_hashtags_data:
        key = hashtag.split(":")[1]
        value = redis.get(hashtag)
        result = str(key) + "|" + str(value) + "|%*%|"
        trending_hashtags += result
    yield 'data: %s\n\n' % trending_hashtags[0:(len(trending_hashtags)-5)]


def print_sliding_window_result(key, metadata, record):
    print(str(record['userName']) + " - " + str(record['tweetText']) + " -- " + str(record['tweetDateTime']))


def trending_hash_tags_aerospike():
    query = client.query(AEROSPIKE_NAMESPACE,AEROSPIKE_SET)
    query.select(TWEET_HASHTAG_BIN)
    query.apply('toptweets', 'top', [10])
    results = query.results()
    trending_hashtags = ""
    for line in results:
        for i in range(0, 10):
            print(str(line[i][TWEET_HASHTAG_BIN]) + "-" + str(line[i][TWEET_COUNT]))
            result = str(line[i][TWEET_HASHTAG_BIN]) + "|" + str(line[i][TWEET_COUNT]) + "|%*%|"
            trending_hashtags += result

    yield 'data: %s\n\n' % trending_hashtags[0:(len(trending_hashtags)-5)]


def tweets_sliding_window_aerospike():
    two_mins_mills = 5 * 60 * 1000
    current_time_milliseconds = time.time() * 1000
    print(current_time_milliseconds)
    time1 = current_time_milliseconds - two_mins_mills - 30
    time2 = current_time_milliseconds - two_mins_mills
    print("get_tweets_in_timeframe from " + str(time1) + " to " + str(time2))
    query = client.query(AEROSPIKE_NAMESPACE, AEROSPIKE_STORMSET)
    query.select(USER_NAME_BIN, TWEET_TEXT_BIN, TWEET_DATETIME_BIN)
    query.where(p.between(TWEET_DATETIME_BIN, time1, time1))
    results = query.results()
    tweets = ""
    for line in results:
        print(str(line['userName']) + " - " + str(line['tweetText']) + " -- " + str(line['tweetDateTime']))
        tweets = tweets + str(line['userName']) + " - " + str(line['tweetText']) + " -- " + str(line['tweetDateTime']) + "|%*%|"

    yield 'data: %s\n\n' % tweets[0:(len(tweets)-4)]


def tweets_sliding_window():
    two_mins_mills = 5 * 60 * 1000
    current_time_milliseconds = time.time() * 1000
    time1 = current_time_milliseconds - two_mins_mills - 30
    time2 = current_time_milliseconds - two_mins_mills

    tweet_ids = redis.zrangebyscore("tweet-time-series", round(current_time_milliseconds - two_mins_mills), round(current_time_milliseconds))

    tweets_data = redis.mget(tweet_ids)
    print(len(tweets_data))
    tweets = ""
    for tweet in tweets_data:
        tweets = tweets + str(tweet) + "|%*%|"
    yield 'data: %s\n\n' % tweets[0:(len(tweets)-4)]


@app.route('/trending-hashtags')
def show_trending_hashtags():
    return render_template("trending_hashtags.html")

@app.route('/')
def show_homepage():
    return render_template("index.html")

@app.route('/redis-hashtags-stream')
def redis_trending_stream():
    return Response(trending_hash_tags_redis(), mimetype="text/event-stream")

@app.route('/aerospike-hashtags-stream')
def aerospike_trending_stream():
    return Response(trending_hash_tags_aerospike(), mimetype="text/event-stream")

# @app.route('/tweets-stream')
# def tweets_stream():
#     return Response(tweets_sliding_window(), mimetype="text/event-stream")
#

if __name__ == '__main__':
    app.run(threaded=True,
    host='0.0.0.0'
)