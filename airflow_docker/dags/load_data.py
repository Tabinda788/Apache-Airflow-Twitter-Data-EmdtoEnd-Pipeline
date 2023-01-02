import psycopg2

conn = psycopg2.connect(
    host="172.17.0.1",
    port = "15433",
    database="test",
    user="admin",
    password="admin")


cur = conn.cursor()


sql2 = '''copy public.twitter_table_data1(index,tweets,created_at,\
reply_count,retweet_count,like_count,quote_count,domain_names,entity_names)
FROM '/data/elonmusk_tweets.csv'
DELIMITER ','
CSV HEADER;'''
cur.execute(sql2)
sql3 = '''select * from twitter_table_data1;'''
cur.execute(sql3)
for i in cur.fetchall():
    print(i)

conn.commit()

conn.close()
print('Connection closed')
