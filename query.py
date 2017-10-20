#!/usr/bin/env python
import config, argparse, uuid, pymongo
from google.cloud import bigquery

def query(query, db, col):
    conn = pymongo.MongoClient()[db][col]
    client = bigquery.Client()
    query_job = client.run_async_query(str(uuid.uuid4()), query)
    query_job.begin()
    query_job.result()
    destination_table = query_job.destination
    destination_table.reload()
    headers = [field.name for field in destination_table.schema]
    cont = 0
    for row in destination_table.fetch_data():
        conn.save(dict(zip(headers, row)))
        cont += 1
        print(cont)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('query', help='BigQuery SQL Query.')
    parser.add_argument('--db', help='MongoDB database.')
    parser.add_argument('--col', help='MongoDB collection.')
    args = parser.parse_args()
    query(args.query, args.db if args.db else config.db, args.col if args.col else config.col)