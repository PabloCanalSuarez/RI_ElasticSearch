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

    validatedDrugs = getDrugs()
    es = Elasticsearch()

    count=0
    for doc in results["aggregations"]["Terminos mas significativos"]["buckets"]:
               if(validate(doc["key"],validatedDrugs)):
                    count=count+1
                    print ("VALIDADO:"+doc["key"])
    print (count)

def getDrugs():
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    sparql.setQuery("""
       SELECT ?moleculeLabel
    WHERE
    {
    	?molecule  wdt:P31 wd:Q12140
    	; wdt:P274 ?formule
    	; wdt:P117 ?picture
    	SERVICE wikibase:label {  bd:serviceParam wikibase:language "en, de" . }
    }
    ORDER BY ?moleculeLabel
    """)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()

def validate(name,results):
    for result in results["results"]["bindings"]:
        if(result["moleculeLabel"]["value"]==name):
            return True
    return False

if __name__ == '__main__':
    main()

