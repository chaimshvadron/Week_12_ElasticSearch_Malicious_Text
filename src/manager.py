from load_data import ElasticDataLoader
from process_data import SentimentProcessor
from delete_unwanted import UnwantedDeleter

class ElasticManager:
    def __init__(self, es_client, index_name="tweets"):
        self.es = es_client
        self.index_name = index_name


    def run(self, csv_file, weapons_file):

        mapping = {
            "mappings": {
                "properties": {
                    "TweetID": {"type": "keyword"},
                    "CreateDate": {"type": "date", "format": "yyyy-MM-dd HH:mm:ssXXX||EEE MMM dd HH:mm:ss Z yyyy"},
                    "Antisemitic": {"type": "boolean"},
                    "text": {"type": "text"},
                    "sentiment": {"type": "keyword"}, 
                    "weapons": {"type": "keyword"}  
                }
            }
        }
        
        loader = ElasticDataLoader(self.es, self.index_name, mapping)
        loader.create_index()
        data = loader.load_csv(csv_file)
        loader.bulk_index(data)

        processor = SentimentProcessor(self.es, self.index_name)
        with open(weapons_file, 'r', encoding='utf-8') as f:
            weapons_list = [line.strip() for line in f if line.strip()]
    
        processor.process_all(weapons_list)
        print("Analysis completed successfully")

        deleter = UnwantedDeleter(self.es, self.index_name)
        deleter.delete_unwanted()
        
