from flask import Flask, Response, render_template
from rediscluster import RedisCluster
import time
import json

app = Flask(__name__)
startup_nodes = [{"host": "10.0.0.30", "port": "7000"}]
redis = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)


def trending_hash_tags():
    trending_hashtags_data = redis.zrange("trending-topics", -20, -1)
    trending_hashtags = ""
    for hashtag in trending_hashtags_data:
        key = hashtag.split(":")[1]
        value = redis.get(hashtag)
        result = str(key) + "|" + str(value) + "|%*%|"
        trending_hashtags += result
    yield 'data: %s\n\n' % trending_hashtags[0:(len(trending_hashtags)-5)]


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


@app.route('/')
def show_homepage():
    return render_template("index.html")


@app.route('/hashtags-stream')
def trending_stream():
    return Response(trending_hash_tags(), mimetype="text/event-stream")


@app.route('/tweets-stream')
def tweets_stream():
    return Response(tweets_sliding_window(), mimetype="text/event-stream")


if __name__ == '__main__':
    app.run(threaded=True,
    host='0.0.0.0'
)