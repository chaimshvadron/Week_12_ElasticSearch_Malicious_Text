from elasticsearch import Elasticsearch

class QueryService:
    def __init__(self, es_client: Elasticsearch, index_name: str):
        self.es = es_client
        self.index_name = index_name
    
    def get_antisemitic_with_weapons(self):
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"Antisemitic": True}},
                        {"script": {
                            "source": "doc['weapons'].size() > 0",
                            "lang": "painless"
                        }}
                    ]
                }
            }
        }
        
        response = self.es.search(index=self.index_name, body=query, size=100)
        return response["hits"]["hits"]
    
    def get_documents_with_multiple_weapons(self, min_weapons=2):
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"script": {
                            "source": f"doc['weapons'].size() >= {min_weapons}",
                            "lang": "painless"
                        }}
                    ]
                }
            }
        }
        
        response = self.es.search(index=self.index_name, body=query, size=100)
        return response["hits"]["hits"]


# Example usage
if __name__ == "__main__":
    es = Elasticsearch("http://localhost:9200")
    index_name = "tweets"
    
    query_service = QueryService(es, index_name)
    
    # Example: Get antisemitic documents with weapons
    antisemitic_docs = query_service.get_antisemitic_with_weapons()
    print(f"Found {len(antisemitic_docs)} antisemitic documents with weapons")
    
    # Example: Get documents with multiple weapons
    multi_weapon_docs = query_service.get_documents_with_multiple_weapons(min_weapons=2)
    print(f"Found {len(multi_weapon_docs)} documents with 2+ weapons")
