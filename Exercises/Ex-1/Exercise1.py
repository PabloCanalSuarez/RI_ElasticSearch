#-------------------------------------------------------------------------------
# Author:      Clara and Pablo
#-------------------------------------------------------------------------------

import json
import sys
from datetime import datetime

from elasticsearch import Elasticsearch

def main():
    es = Elasticsearch()
    ## Obtain significant terms
    getSignificantTerms(es)

    ## Exercise 1
    ## Query All significant terms
    getDocuments_AllTerms(es)
    getDocuments_FirstTerms(es)
    getDocuments_LastTerms(es)

def getSignificantTerms(es):
    results = es.search(
        index="reddit-mentalhealth4",
        body = {
            "size":0,
            "query": {
                "match": {
                  "subreddit": "stopsmoking"
                }
              },
              "aggs": {
                "Terminos significativos": {
                  "significant_terms": {
                    "gnd": {},
                    "field": "selftext",
                    "size": 25
                  }
                }
              }
        }
    )
    saveSignificantTermsFile(results, "significant_terms.txt")


def getDocuments_AllTerms(es):
    results = es.search(
        index="reddit-mentalhealth4",
        body = {

          "_source": ["author","created_utc","selftext"],
          "query": {
            "bool": {
              "must": [
                {
                  "match": {
                    "subreddit": "stopsmoking"
                  }
                },
                {
                  "match": {
                    "selftext": {
                      "query": "smoke smoker crave cigarrette quit nicotin pack",
                      "operator": "or"
                    }
                  }
                }
              ]
            }
          },
          "size": 100
        }
    )
    saveFile(results, "1_All_significant_terms.txt")

def getDocuments_FirstTerms(es):
    results = es.search(
        index="reddit-mentalhealth4",
        body = {

          "_source": ["author","created_utc","selftext"],
          "query": {
            "bool": {
              "must": [
                {
                  "match": {
                    "subreddit": "stopsmoking"
                  }
                },
                {
                  "match": {
                    "selftext": {
                      "query": "smoke smoker crave cigarrette",
                      "operator": "or"
                    }
                  }
                }
              ]
            }
          },
          "size": 100
        }
    )
    saveFile(results, "1_First_significant_terms.txt")

def getDocuments_LastTerms(es):
    results = es.search(
        index="reddit-mentalhealth4",
        body = {

          "_source": ["author","created_utc","selftext"],
          "query": {
            "bool": {
              "must": [
                {
                  "match": {
                    "subreddit": "stopsmoking"
                  }
                },
                {
                  "match": {
                    "selftext": {
                      "query": "cigarrette quit nicotin pack",
                      "operator": "or"
                    }
                  }
                }
              ]
            }
          },
          "size": 100
        }
    )
    saveFile(results, "1_Last_significant_terms.txt")

def saveFile(results, nameFile):
    f = open(nameFile,"w+",encoding='utf8') ## file to save
    i = 1
    for x in results["hits"]["hits"]:
        f.write("Document: %d \n" % i)
        for y in x["_source"]:
            f.write("%s: %s\n" %(y,x["_source"][y]))
        f.write("\n")
        f.write("---------\n")
        i += 1
    f.close()

def saveSignificantTermsFile(results, nameFile):
    f = open(nameFile,"w+",encoding='utf8') ## file to save
    for x in results["aggregations"]["Terminos significativos"]["buckets"]:
        f.write("%s \n\t--> Score: %f" %(x["key"], x["score"]))
        f.write("\n\n")
    f.close()



if __name__ == '__main__':
    main()
