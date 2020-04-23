"""
Dynamo to SQS
"""

import boto3
import json

import tweepy
from tweepy import OAuthHandler

API_KEY = "***"
API_SECRET = "***"
# Replace the API_KEY and API_SECRET with your application's key and secret.
auth = OAuthHandler(API_KEY, API_SECRET)

api = tweepy.API(auth, wait_on_rate_limit=True,wait_on_rate_limit_notify=True)

if (not api):
    print ("Can't Authenticate")
  

DYNAMODB = boto3.resource('dynamodb')
TABLE = "since_tweet"
QUEUE = "producer"
SQS = boto3.client("sqs")



def scan_table(table):
    """Scans table and return results"""
    
    producer_table = DYNAMODB.Table(table)
    response = producer_table.scan()
    items = response['Items']
    return items

def send_sqs_msg(msg, queue_name, delay=0):
    """Send SQS Message
    Expects an SQS queue_name and msg in a dictionary format.
    Returns a response dictionary. 
    """

    queue_url = SQS.get_queue_url(QueueName=queue_name)["QueueUrl"]
    queue_send_log_msg = "Send message to queue url: %s, with body: %s" %\
        (queue_url, msg)
    json_msg = json.dumps(msg)
    response = SQS.send_message(
        QueueUrl=queue_url,
        MessageBody=json_msg,
        DelaySeconds=delay)
    queue_send_log_msg_resp = "Message Response: %s for queue url: %s" %\
        (response, queue_url) 
    return response

def send_emissions(table, queue_name):
    """Send Emissions"""
    
    items = scan_table(table=table)
    for item in items:
        handle = item['screen_name']
        since_id_item = item['since_id']
        tweet_data = api.user_timeline(screen_name=handle, since_id = since_id_item, tweet_mode='extended')
        tweet_id = []
        for row in tweet_data:
            text_msg = row._json['full_text']
            tweet_id.append(row._json['id'])
            msg_dict = {"text":text_msg, 'handle':handle}
            response = send_sqs_msg(msg_dict, queue_name=queue_name)
        if len(tweet_id)>1:
            max_id = max(tweet_id)
            item['since_id'] = max_id
            producer_table = DYNAMODB.Table(table)
            print('Updating since_id to'+str(max_id))
            _ = producer_table.put_item(Item=item)
        


def lambda_handler(event, context):
    """
    Lambda entrypoint
    """

    extra_logging = {"table": TABLE, "queue": QUEUE}
    send_emissions(table=TABLE, queue_name=QUEUE)