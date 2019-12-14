#-------------------------------------------------------------------------------
# Author:      Clara and Pablo
#-------------------------------------------------------------------------------

import json # Para poder trabajar con objetos JSON
import pprint # Para poder hacer uso de PrettyPrinter
import sys # Para poder usar exit

from elasticsearch import Elasticsearch

def main():
    pp = pprint.PrettyPrinter(indent=2)
    es = Elasticsearch()

    ## Obtain significant terms
##    results = es.search(
##        index="reddit-mentalhealth4",
##        body = {
##            "query": {
##                "match": {
##                  "subreddit": "stopsmoking"
##                }
##              },
##              "aggs": {
##                "Terminos significativos": {
##                  "significant_terms": {
##                    "field": "selftext",
##                    "size": 25
##                  }
##                }
##              }
##        }
##    )

##    print(str(results["hits"]["total"]) + " resultados para una query")
##    pp.pprint(results);
##    print(results);


    ## Query Ex.1
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
    f = open("results.txt","w+") ## file to save
    i = 1
    for x in results["hits"]["hits"]:
        f.write("Document: %d \n" % i)
        for y in x["_source"]:
            f.write("%s: %s\n\t" %(y,x["_source"][y]))
        f.write("\n")
        f.write("---------")
        i += 1
    f.close()


if __name__ == '__main__':
    main()
