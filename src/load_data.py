from elasticsearch import Elasticsearch, helpers
import pandas as pd

es = Elasticsearch("http://localhost:9200")

INDEX_NAME = "tweets"

mapping = {
    "mappings": {
        "properties": {
            "TweetID": {"type": "keyword"},
            "CreateDate": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss+00:00||yyyy-MM-dd"},
            "Antisemitic": {"type": "integer"},
            "text": {"type": "text"},
            "sentiment": {"type": "keyword"}, 
            "weapons": {"type": "keyword"}  
        }
    }
}

if not es.indices.exists(index=INDEX_NAME):
    es.indices.create(index=INDEX_NAME, body=mapping)
    print(f"Index '{INDEX_NAME}' created successfully.")
else:
    print(f"Index '{INDEX_NAME}' already exists.")

# Load CSV data
csv_file = "data/tweets_injected 3.csv"
df = pd.read_csv(csv_file)

data = df.to_dict(orient="records")

# Bulk index with helpers
success, failed = helpers.bulk(es, data, index=INDEX_NAME, raise_on_error=False)
print(f"Successfully indexed {success} documents.")
