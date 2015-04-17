import aerospike
import os

AEROSPIKE_NAMESPACE = "test"
AEROSPIKE_SET = "stormset-hashtag"
TWEET_HASHTAG_BIN = "hashTag"
TWEET_COUNT = "hashTagCount"


def get_config():
    config = {
        'hosts': [
            ('127.0.0.1', 3000)
        ],
        'policies': {
            'timeout': 1000
        },
        'lua': {
            'user_path': os.path.dirname('udf/toptweets.lua')
        }
    }
    return config


def create_aerospike_client(config):
    client = aerospike.client(config)
    client.connect()
    return client


def print_result((record)):
    for line in record:
        print(str(line[TWEET_HASHTAG_BIN]) + "-" + str(line[TWEET_COUNT]))


def main():
    config = get_config()
    client = create_aerospike_client(config)
    query = client.query(AEROSPIKE_NAMESPACE,AEROSPIKE_SET)
    query.select(TWEET_HASHTAG_BIN)
    query.apply('toptweets', 'top', [10])
    query.foreach(print_result)


main()




