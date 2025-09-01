from elasticsearch import Elasticsearch
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

class SentimentProcessor:
    def __init__(self, es_client: Elasticsearch, index_name: str):
        self.es = es_client
        self.index_name = index_name
        nltk_dir = "/tmp/nltk_data"
        import os
        os.makedirs(nltk_dir, exist_ok=True)
        nltk.data.path.append(nltk_dir)
        nltk.download('vader_lexicon', download_dir=nltk_dir, quiet=True)
        self.sia = SentimentIntensityAnalyzer()
    
    def add_sentiment(self):
        query = {"query": {"match_all": {}}}
        response = self.es.search(index=self.index_name, body=query, size=10000)
        docs = response['hits']['hits']
        
        for doc in docs:
            text = doc['_source']['text']
            sentiment_score = self.sia.polarity_scores(text)['compound']
            if sentiment_score > 0.1:
                sentiment = "positive"
            elif sentiment_score < -0.1:
                sentiment = "negative"
            else:
                sentiment = "neutral"
            
            self.es.update(index=self.index_name, id=doc['_id'], body={
                "doc": {"sentiment": sentiment}
            })
        
        print(f"Added sentiment to {len(docs)} documents.")

# Usage
if __name__ == "__main__":
    es = Elasticsearch("http://localhost:9200")
    index_name = "tweets"
    
    processor = SentimentProcessor(es, index_name)
    processor.add_sentiment()
