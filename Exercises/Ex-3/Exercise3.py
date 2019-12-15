#-------------------------------------------------------------------------------
# Authors:      Clara and Pablo
#-------------------------------------------------------------------------------

import json
import sys
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers


from SPARQLWrapper import SPARQLWrapper, JSON
import json # Para poder trabajar con objetos JSON
import pprint
import sys



def main():

##    validatedDrugs = getDrugs()
    es = Elasticsearch()

    results = es.search(
        index="reddit-mentalhealth4",
        body = {
          "query": {
            "query_string": {
              "default_field": "selftext",
              "query": "prescribed|mg|mL"
            }
          },
          "aggs": {
            "Terminos mas significativos": {
              "significant_terms": {
                "field": "selftext",
                "gnd": {},
                "size":3000
              }
            }
          }
        },
        request_timeout=50
    )

    count=0
    for doc in results["aggregations"]["Terminos mas significativos"]["buckets"]:
        if(validate(doc["key"])):
            count+=1
            print ("VALIDADO:"+doc["key"])
    print(count)
    print("Program terminated.")


def validate(keyword):
    import requests

    q12140 = 'Q12140'
    p31 = 'P31'

    r = requests.get('https://www.wikidata.org/w/api.php?action=wbsearchentities&search='+keyword+'&language=en&format=json')
    data = r.json()

    ids = []
    for x in data["search"]:
        ids.append(x["id"])

    for id in ids:

        r2 = requests.get('https://www.wikidata.org/w/api.php?action=wbgetentities&ids='+id+'&languages=en&format=json')

        dataCheck = r2.json()

        isValid = dataCheck["entities"][id]["claims"]
        for item in isValid:
            if(item == "P31"):
                for x in isValid["P31"]:
                    checkedId = x["mainsnak"]["datavalue"]["value"]["id"]
                    if(checkedId == q12140):
                        return True

    return False

if __name__ == '__main__':
    main()

