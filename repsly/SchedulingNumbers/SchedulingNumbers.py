import pandas as pd
from pymongo import MongoClient

client = MongoClient('mongodb+srv://markolinke:zaWz0NDNTqjmVS7b@planning-production-cluster-c52pu.mongodb.net/test')

db = client['schedules']
col_alerts = db['Alert']

print(col_alerts.find_one())
print(col_alerts.find_one({"_id": "07a439b4-ae5e-4d73-af93-998e6d624fbd"}))
print(col_alerts.find_one({"AlertId": "07a439b4-ae5e-4d73-af93-998e6d624fbd"}))
