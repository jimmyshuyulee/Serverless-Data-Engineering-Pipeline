# Real time twitter sentiment analysis with AWS

In this project I have created a serverless data engineering pipeline to download tweets from users and analyze the tweet sentiment using AWS comprehend API. The architecture for the pipeline is shown below. 

![](https://user-images.githubusercontent.com/58792/55354483-bae7af80-547a-11e9-9909-a5621251065b.png)


The cloud watch timer is used to invoke the producer lambda function at regular intervals of time. Here for illustration purpose, I have used a trigger rate of 1 minute. When triggered by cloud watch, the producer lambda function reads data from the dynamoDB table. 

The dynamo DB also keeps track of the last tweet id that we have analyzed for each user. This is to ensure that we not downloading and analyzing same tweets again. Once we scan the table, the producer downloads if any new tweets are available for all the users and if new any tweets are available, downloads and pushes them in SQS. As can be seen in the architecture whenever any data is pushed to the SQS it triggers a consumer lambda function. This function uses comprehend API to analyze the sentiment of the tweets and pushes the results into a predefined S3 bucket. 
