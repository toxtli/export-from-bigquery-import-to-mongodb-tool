# bigquery-to-mongodb-extractor

> pip install -r requirements.txt

Example:

In order to execute the following query you need to create a dataset on this URL: https://bigquery.cloud.google.com/dataset/fh-bigquery:reddit_comments

> ./query.py 'SELECT * FROM [fh-bigquery:reddit_comments.2016_07] WHERE subreddit="The_Donald" AND author <> "[deleted]" LIMIT 10'

For help run:

> ./query.py --help
