from elasticsearch import Elasticsearch

class UnwantedDeleter:
    def __init__(self, es_client: Elasticsearch, index_name: str):
        self.es = es_client
        self.index_name = index_name
    
    def delete_unwanted(self):
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"Antisemitic": False}},
                        {"terms": {"sentiment": ["neutral", "positive"]}},
                        {
                            "script": {
                                "source": "doc['weapons'].size() == 0",
                                "lang": "painless"
                            }
                        }
                    ]
                }
            }
        }
        
        response = self.es.delete_by_query(index=self.index_name, body=query)
        print(f"Deleted {response['deleted']} unwanted documents.")
        return response
