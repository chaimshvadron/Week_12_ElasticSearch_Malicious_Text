from fastapi import FastAPI
from elasticsearch import Elasticsearch
from query_service import QueryService
from manager import ElasticManager

app = FastAPI()

es = Elasticsearch("http://localhost:9200")
index_name = "tweets"

query_service = QueryService(es, index_name)

csv_file = "data/tweets_injected3.csv"
weapons_file = "data/weapon_list.txt"

@app.on_event("startup")
def startup_process_data():
    try:
        print("Starting data processing...")
        manager = ElasticManager(es_client=es, index_name=index_name)
        manager.run(csv_file, weapons_file)
        print("Data processing completed successfully")
    except Exception as e:
        print(f"Error processing data: {str(e)}")

@app.get("/antisemitic-with-weapons")
def get_antisemitic_with_weapons():
    try:
        results = query_service.get_antisemitic_with_weapons()
        documents = [doc["_source"] for doc in results]
        return {"documents": documents}
    except Exception as e:
        return {"message": f"Data not ready or not available: {str(e)}"}

@app.get("/documents-with-multiple-weapons")
def get_documents_with_multiple_weapons():
    try:
        results = query_service.get_documents_with_multiple_weapons(min_weapons=2)
        documents = [doc["_source"] for doc in results]
        return {"documents": documents}
    except Exception:
        return {"message": "Data not ready or not available"}
