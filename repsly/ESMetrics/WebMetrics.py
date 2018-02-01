from elasticsearch import Elasticsearch
import csv
import os
import math
import pandas as pd

es = Elasticsearch(hosts='elastic.pacman.zg.reps.ly:80')
filename = '/Users/markolinke/es_dump.csv'
page_size = 100

if os.path.exists(filename):
    os.remove(filename)

with open(filename, 'w') as f:
    fieldnames = ['id', 'responseSize', 'companyId', 'companyName', 'userId', 'path', 'userName',
                  'exportedRows', 'dBQueryLength', 'totalRequestLength',
                  'extraInfo', 'dateBegin', 'dateEnd', 'daysInterval', 'createdDateTimeUTC']

    w = csv.DictWriter(f, quoting=csv.QUOTE_ALL, fieldnames=fieldnames, dialect=csv.excel)
    w.writeheader()

    res = es.search(index='metrics', doc_type='webexports', body={"query": {"match_all": {}}, "size": 0})
    total = res['hits']['total']

    page_count = int(math.ceil(total / page_size))
    for page_num in range(page_count):
        print('page', page_num)

        res = es.search(
            index='metrics',
            doc_type='webexports',
            body={
                "query": {
                    "match_all": {}
                },
                "size": page_size,
                "from": 0}
        )

        for item in res['hits']['hits']:
            w.writerow(item['_source'])

print('done with csv export')

df = pd.read_csv(filename)
print(df.head())