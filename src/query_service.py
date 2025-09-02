from elasticsearch import Elasticsearch

class QueryService:
    def __init__(self, es_client: Elasticsearch, index_name: str):
        self.es = es_client
        self.index_name = index_name
    
    def get_antisemitic_with_weapons(self):
        self.es.indices.refresh(index=self.index_name)
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"Antisemitic": True}}
                    ],
                    "filter": [
                        {
                            "script": {
                                "script": {
                                    "source": "doc['weapons'].length > 0",
                                    "lang": "painless"
                                }
                            }
                        }
                    ]
                }
            }
        }
        
        response = self.es.search(index=self.index_name, body=query, size=100)
        return response["hits"]["hits"]
    
    def get_documents_with_multiple_weapons(self, min_weapons=2):
        self.es.indices.refresh(index=self.index_name)
        query = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "script": {
                                "script": {
                                    "source": f"doc['weapons'].length >= {min_weapons}",
                                    "lang": "painless"
                                }
                            }
                        }
                    ]
                }
            }
        }
        
        response = self.es.search(index=self.index_name, body=query, size=100)
        return response["hits"]["hits"]

