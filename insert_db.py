from elasticsearch import helpers
from elasticsearch import Elasticsearch
import csv

es = Elasticsearch("http://localhost:9200")

def importDataToElastic():
       index_name = "ela"

       with open("/opt/airflow/dags/comparacao_brasil_inglaterra.csv", "r") as fi:
           reader = csv.DictReader(fi, delimiter=",")
           actions = []
           for row in reader:
               action = {"index": {"_index": index_name, "_id": int(row[""])}}
               doc = {
                   "home_team": row["home_team"],
                   "away_team": row["away_team"],
                   "home_goals": row["home_goals"],
                   "away_goals": row["away_goals"],
                   "season": int(row["season"])
                } 
               actions.append(action)
               actions.append(doc)
       es.bulk(index=index_name, operations=actions)
              
       result = es.count(index=index_name)
       print(result.body['count'])

importDataToElastic()       