# python query.py 'SELECT * FROM [fh-bigquery:reddit_comments.2016_07] WHERE subreddit="The_Donald" AND author <> "[deleted]"' 

import argparse, uuid, pymongo
from google.cloud import bigquery

conn = pymongo.MongoClient()["test"]["trump"]
headers = ["body","score_hidden","archived","name","author","author_flair_text","downs","created_utc","subreddit_id","link_id","parent_id","score","retrieved_on","controversiality","gilded","id","subreddit","ups","distinguished","author_flair_css_class"]

def query(query):
    client = bigquery.Client()
    query_job = client.run_async_query(str(uuid.uuid4()), query)
    query_job.begin()
    query_job.result()
    destination_table = query_job.destination
    destination_table.reload()
    cont = 0
    for row in destination_table.fetch_data():
        record = dict(zip(headers, row))
        record["_id"] = str(record["created_utc"]) + str(record["link_id"])
        conn.save(record)
        cont += 1
        print(cont)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('query', help='BigQuery SQL Query.')
    args = parser.parse_args()
    query(args.query)
