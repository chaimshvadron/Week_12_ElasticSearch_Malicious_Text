from elasticsearch import Elasticsearch, helpers
import pandas as pd

class ElasticDataLoader:

    def __init__(self, es_client: Elasticsearch, index_name: str, mapping: dict):
        self.es = es_client
        self.index_name = index_name
        self.mapping = mapping
    
    def create_index(self):
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name, body=self.mapping)
            print(f"Index '{self.index_name}' created successfully.")
        else:
            self.es.indices.delete(index=self.index_name)
            self.es.indices.create(index=self.index_name, body=self.mapping)
            print(f"Index '{self.index_name}' recreated.")
    
    def load_csv(self, csv_file: str) -> list:
        df = pd.read_csv(csv_file)
        return df.to_dict(orient="records")
    
    def bulk_index(self, data: list):
        success, failed = helpers.bulk(self.es, data, index=self.index_name, raise_on_error=False)
        print(f"Successfully indexed {success} documents.")
        if failed:
            print(f"Failed to index {len(failed)} documents.")
            for fail in failed[:5]: 
                print(f"Error: {fail}")
        return success, failed

# Usage
if __name__ == "__main__":
    es = Elasticsearch("http://localhost:9200")
    
    INDEX_NAME = "tweets"
    mapping = {
        "mappings": {
            "properties": {
                "TweetID": {"type": "keyword"},
                "CreateDate": {"type": "date", "format": "yyyy-MM-dd HH:mm:ssXXX||EEE MMM dd HH:mm:ss Z yyyy"},
                "Antisemitic": {"type": "integer"},
                "text": {"type": "text"},
                "sentiment": {"type": "keyword"}, 
                "weapons": {"type": "keyword"}  
            }
        }
    }
    
    loader = ElasticDataLoader(es, INDEX_NAME, mapping)
    loader.create_index()
    
    csv_file = "data/tweets_injected 3.csv"
    data = loader.load_csv(csv_file)
    
    loader.bulk_index(data)
