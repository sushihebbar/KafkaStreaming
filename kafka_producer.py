import random
from kafka import KafkaProducer
import mysql.connector
import time

# connect to the MySQL database
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="insta_db"
)

# create a cursor to execute SQL queries
cursor = mydb.cursor()

# initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# continuously publish new captions and hashtags to Kafka
while True:
    try:
        # select a random number of captions to retrieve
        num_captions = random.randint(1, 25)
        
        #----------------------------------------------------------------------------------

        # select all captions that were added since the last time the loop ran
        sql_captions = "SELECT caption_id, caption, date_time, language FROM captions WHERE date_time > '2021-01-01 19:51:27 EDT' - INTERVAL 10 SECOND LIMIT 0, 25;"
        cursor.execute(sql_captions)
        # print the SQL query being executed
        print("Executing SQL query for captions...")
        # publish each new captions to the Kafka topic
        for caption_id, caption, date_time, language in cursor:
            
            message = bytes(f"{caption_id},{caption},{date_time},{language}", encoding='utf-8')
        
            # print the data being processed
            print("Publishing caption:", caption)
        
            # publish the message to the Kafka topic
            producer.send('captions', value=message)

        #----------------------------------------------------------------------------------

        # select a random number of hashtags to retrieve
        num_hashtags = random.randint(1, 10)

        # select all new hashtags that were added since the last time the loop ran
        sql_hashtags = "SELECT hashtag_id, hashtag FROM hashtags WHERE hashtag_id < (SELECT MAX(hashtag_id) FROM captions_hashtags)"
        cursor.execute(sql_hashtags)

        # print the SQL query being executed
        print("Executing SQL query for hashtags...")

        # publish each new hashtag to the Kafka topic
        for hashtag_id, hashtag in cursor:
           # serialize the row data to bytes
            message = bytes(f"{hashtag_id},{hashtag}", encoding='utf-8')
            
            # print the data being processed
            print("Publishing hashtag:", hashtag)
            # publish the message to the Kafka topic
            producer.send('hashtags', value=message)

        #----------------------------------------------------------------------------------

        # select the top 10 captions with the most hashtags
        sql_top_captions = "SELECT t.caption_id, t.caption, t.date_time, t.language, COUNT(th.hashtag_id) as hashtag_count FROM captions t JOIN captions_hashtags th ON t.caption_id = th.caption_id GROUP BY  t.caption_id, t.caption, t.date_time, t.language ORDER BY hashtag_count DESC LIMIT 10;"
        cursor.execute(sql_top_captions)

        # print the SQL query being executed
        print("Executing SQL query for top captions...")

        # publish each top caption to the Kafka topic
        for caption_id, caption, date_time, language, hashtag_count in cursor:
             # serialize the row data to bytes
            message = bytes(f"{caption_id},{caption},{date_time},{language},{hashtag_count}", encoding='utf-8')
            
            # print the data being processed
            print("Publishing top caption:", caption)
            # publish the message to the Kafka topic
            producer.send('top_captions', value=message)

        #----------------------------------------------------------------------------------

        # select the hashtags that appear in the top 10 captions
        
        sql_top_hashtags = "SELECT h.hashtag_id, h.hashtag, SUM(th.count) AS count FROM hashtags h JOIN captions_hashtags th ON h.hashtag_id = th.hashtag_id JOIN (SELECT t.caption_id FROM captions t JOIN captions_hashtags th ON t.caption_id = th.caption_id GROUP BY t.caption_id ORDER BY COUNT(th.hashtag_id) DESC LIMIT 10) AS subquery ON th.caption_id = subquery.caption_id GROUP BY h.hashtag_id, h.hashtag ORDER BY count DESC LIMIT 10;"
        
        # execute the SQL query to retrieve the top hashtags
        cursor.execute(sql_top_hashtags)

        # print the SQL query being executed
        print("Executing SQL query for top hashtags...")

        # publish each top hashtag to the Kafka topic
        for hashtag_id, hashtag, count in cursor:
            # serialize the row data to bytes
            message = bytes(f"{hashtag_id},{hashtag},{count}", encoding='utf-8')
            
            # print the data being processed
            print("Publishing top hashtag:", hashtag)
            # publish the message to the Kafka topic
            producer.send('top_hashtags', value=message)
        
        #----------------------------------------------------------------------------------

        # sleep for a random amount of time between 5 and 15 seconds
        time.sleep(random.randint(5, 15))
        
    except Exception as e:
        print(e)
        # in case of any error, sleep for a shorter time and try again
        time.sleep(2)
