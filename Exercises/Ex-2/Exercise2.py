#-------------------------------------------------------------------------------
# Authors:      Clara and Pablo
#-------------------------------------------------------------------------------

import json
import sys
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers

def main():
    es = Elasticsearch()

    termsGnd = getSignificantTermsMoreLikeThis(es)



def getSignificantTermsMoreLikeThis(es):
    results = es.search(
        index="reddit-mentalhealth4",
        body = {

          "query": {
            "more_like_this": {
              "fields": [
                "selftext"
              ],
              "like": [
                "I was diagnosed with schizophrenia",
                "hallucinat",
                "dellu"
              ],
              "min_term_freq": 1,
              "max_query_terms": 12,
              "boost": 9
            }
          },
          "size": 1000
        }

    )
    saveFile(results, "results.txt")


def saveFile(results, nameFile):
    f = open(nameFile,"w+",encoding='utf8') ## file to save
    i = 1

    for x in results["hits"]["hits"]:
        f.write("Document: %d \n" % i)

        author = "author: " + x["_source"]["author"]
        creationDate = "creation date: "+ datetime.utcfromtimestamp(int(x["_source"]["created_utc"])).strftime('%Y-%m-%d %H:%M:%S')
        selftext = "selftext: "+ x["_source"]["selftext"]

        line = author +"\n"+ creationDate + "\n"+selftext
        f.write(line)
        f.write("\n")
        f.write("---------\n")
        i += 1

    f.close()

def saveSignificantTermsFile(results, nameFile):
    f = open(nameFile,"w+",encoding='utf8') ## file to save
    for x in results["aggregations"]["Terminos significativos"]["buckets"]:
        if((not x["key"].startswith("_")) and (not x["key"].endswith("_"))):
            f.write("%s \n\t--> Score: %f" %(x["key"], x["score"]))
            f.write("\n\n")
    f.close()



if __name__ == '__main__':
    main()
