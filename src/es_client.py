from elasticsearch import Elasticsearch

# Create connection to locally running Elasticsearch
es = Elasticsearch("http://localhost:9200")

# Check connection
if es.ping():
    print("Successfully connected to Elasticsearch!")
    info = es.info()
    print(f"Version: {info['version']['number']}")
else:
    print("Error connecting to Elasticsearch. Check if it's running on port 9200.")
