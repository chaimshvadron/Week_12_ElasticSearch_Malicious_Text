from elasticsearch import Elasticsearch, helpers
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
import os

class SentimentProcessor:
    def __init__(self, es_client: Elasticsearch, index_name: str):
        self.es = es_client
        self.index_name = index_name
        nltk_dir = "/tmp/nltk_data"
        os.makedirs(nltk_dir, exist_ok=True)
        nltk.data.path.append(nltk_dir)
        nltk.download('vader_lexicon', download_dir=nltk_dir, quiet=True)
        self.sia = SentimentIntensityAnalyzer()
    
    def process_all(self, weapon_list: list):
        self.es.indices.refresh(index=self.index_name)
        print("Starting document processing...")

        query = {"query": {"match_all": {}}}
        response = self.es.search(index=self.index_name, body=query, size=10000)
        docs = response['hits']['hits']
        
        
        

        actions = []
        for doc in docs:
            text = doc['_source']['text']
            text_lower = text.lower()
            
            sentiment_score = self.sia.polarity_scores(text)['compound']
            if sentiment_score > 0.1:
                sentiment = "positive"
            elif sentiment_score < -0.1:
                sentiment = "negative"
            else:
                sentiment = "neutral"
            
            found_weapons = [weapon for weapon in weapon_list if weapon.lower() in text_lower]
            actions.append({
                '_op_type': 'update',
                '_index': self.index_name,
                '_id': doc['_id'],
                'doc': {
                    'sentiment': sentiment,
                    'weapons': found_weapons
                }
            })

        if actions:
            print(f"Processing {len(actions)} documents for sentiment and weapons.")
            helpers.bulk(self.es, actions)
            print("Index refreshed after updates")

