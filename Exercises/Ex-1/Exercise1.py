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

    ## ---------------------
    ##          Gnd
    ## ---------------------
    termsGnd = getSignificantTermsGND(es)

    genTermsGnd7 = helpers.scan(es,
        query = {
            "query": {
                "match": {
                    "selftext": {
                        "query": ""+termsGnd[0]+","+termsGnd[1]+","+termsGnd[2]+","+termsGnd[3]+","+termsGnd[4]+","+termsGnd[5]+","+termsGnd[6],
                        "operator": "or"
                    }
                }
            }
        },
        index="reddit-mentalhealth4"
    )

    genTermsGnd10 = helpers.scan(es,
        query = {
            "query": {
                "match": {
                    "selftext": {
                        "query": ""+termsGnd[0]+","+termsGnd[1]+","+termsGnd[2]+","+termsGnd[3]+","+termsGnd[4]+","+termsGnd[5]+","+termsGnd[6]+","+termsGnd[7]+","+termsGnd[8]+","+termsGnd[9],
                        "operator": "or"
                    }
                }
            }
        },
        index="reddit-mentalhealth4"
    )

    ## Gnd 7
    resultsGnd7 = list(genTermsGnd7)
    for term in termsGnd:
        print(term)
    print("\n ############## Total GND 7 terms: ",len(resultsGnd7),"\n")

    finalDataGnd7 = []
    for x in resultsGnd7:
        line={  "author": x["_source"]["author"],
                "creation date": datetime.utcfromtimestamp(int(x["_source"]["created_utc"])).strftime('%Y-%m-%d %H:%M:%S'),
                "selftext": x["_source"]["selftext"]}
        finalDataGnd7.append(line)


    ## Gnd 10
    resultsGnd10 = list(genTermsGnd10)
    for term in termsGnd:
        print(term)
    print("\n ############## Total GND 10 terms: ",len(resultsGnd10), "\n")

    finalDataGnd10 = []
    for x in resultsGnd10:
        line={  "author": x["_source"]["author"],
                "creation date": datetime.utcfromtimestamp(int(x["_source"]["created_utc"])).strftime('%Y-%m-%d %H:%M:%S'),
                "selftext": x["_source"]["selftext"]}
        finalDataGnd10.append(line)


    ## To save the file collecting with 7 terms and GND
    with open('result_7terms_GND.json', 'w') as f:
        json.dump(finalDataGnd7, f)

    ## To save the file collecting with 10 terms and GND
    with open('result_10terms_GND.json', 'w') as f:
        json.dump(finalDataGnd10, f)





##     ---------------------
##          Chi square
##     ---------------------
    termsChi = getSignificantTermsCHI(es)

    genTermsChi7 = helpers.scan(es,
        query = {
            "query": {
                "match": {
                    "selftext": {
                        "query": ""+termsChi[0]+","+termsChi[1]+","+termsChi[2]+","+termsChi[3]+","+termsChi[4]+","+termsChi[5]+","+termsChi[6],
                        "operator": "or"
                    }
                }
            }
        },
        index="reddit-mentalhealth4"
    )

    genTermsChi10 = helpers.scan(es,
        query = {
            "query": {
                "match": {
                    "selftext": {
                        "query": ""+termsChi[0]+","+termsChi[1]+","+termsChi[2]+","+termsChi[3]+","+termsChi[4]+","+termsChi[5]+","+termsChi[6]+","+termsChi[7]+","+termsChi[8]+","+termsChi[9],
                        "operator": "or"
                    }
                }
            }
        },
        index="reddit-mentalhealth4"
    )


##     Chi 7 ----------------------
    resultsChi7 = list(genTermsChi7)
    for term in termsChi:
        print(term)
    print("\n ############## Total Chi 7 terms: ",len(resultsChi7),"\n")


    finalDataChi7 = []
    for x in resultsChi7:
        line={  "author": x["_source"]["author"],
                "creation date": datetime.utcfromtimestamp(int(x["_source"]["created_utc"])).strftime('%Y-%m-%d %H:%M:%S'),
                "selftext": x["_source"]["selftext"]}
        finalDataChi7.append(line)


##     Chi 10 ----------------------
    resultsChi10 = list(genTermsChi10)
    for term in termsChi:
        print(term)
    print("\n ############## Total Chi 10 terms: ",len(resultsChi10),"\n")


    finalDataChi10 = []
    for x in resultsChi10:
        line={  "author": x["_source"]["author"],
                "creation date": datetime.utcfromtimestamp(int(x["_source"]["created_utc"])).strftime('%Y-%m-%d %H:%M:%S'),
                "selftext": x["_source"]["selftext"]}
        finalDataChi10.append(line)


##     To save the file collecting with 7 terms and GND
    with open('result_7terms_Chi.json', 'w') as f:
        json.dump(finalDataChi7, f)

##     To save the file collecting with 10 terms and GND
    with open('result_10terms_Chi.json', 'w') as f:
        json.dump(finalDataChi10, f)


def getSignificantTermsGND(es):
    results = es.search(
        index="reddit-mentalhealth4",
        body = {
            "size":0,
            "query": {
                "match": {
                  "selftext": "schizophrenia"
                }
              },
              "aggs": {
                "Terminos significativos": {
                  "significant_terms": {
                    "gnd": {},
                    "field": "selftext",
                    "size": 100
                  }
                }
              }
        }
    )
    saveSignificantTermsFile(results, "significant_termsGND.txt") ## save terms file
    terms = []
    for x in results["aggregations"]["Terminos significativos"]["buckets"]:
        if((not x["key"].startswith("_")) and (not x["key"].endswith("_"))):
            terms.append(x["key"])

    return terms

def getSignificantTermsCHI(es):
    results = es.search(
        index="reddit-mentalhealth4",
        body = {
            "size":0,
            "query": {
                "match": {
                  "selftext": "schizophrenia"
                }
              },
              "aggs": {
                "Terminos significativos": {
                  "significant_terms": {
                    "chi_square": {},
                    "field": "selftext",
                    "size": 100
                  }
                }
              }
        }
    )
    saveSignificantTermsFile(results, "significant_termsChi.txt") ## save terms file
    terms = []
    for x in results["aggregations"]["Terminos significativos"]["buckets"]:
        if((not x["key"].startswith("_")) and (not x["key"].endswith("_"))):
            terms.append(x["key"])

    return terms


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
        if((not x["key"].startswith("_")) and (not x["key"].endswith("_"))):
            f.write("%s \n\t--> Score: %f" %(x["key"], x["score"]))
            f.write("\n\n")
    f.close()



if __name__ == '__main__':
    main()
