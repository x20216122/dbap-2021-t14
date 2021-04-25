#!/usr/bin/python
import pymongo
import psycopg2

baseUrl='https://e4ftl01.cr.usgs.gov/MEASURES/GFCC30TC.003/2015.01.01/'
workingDir = '/var/data/david.mcnerney/Project/'
mongoUri = "mongodb://mongo:gmd5PNXNhjGvjp@178.62.41.209:27017"
postgresUri = "postgresql://postgres:gmd5PNXNhjGvjp@178.62.41.209:5432/dbap-2021-t14";

def getCollection(collectionName='indices', database='lp-daac'):
    client = pymongo.MongoClient(mongoUri)
    db = client[database]
    return db[collectionName]

def connect():
    dbConnection = psycopg2.connect(postgresUri)
    dbConnection.set_isolation_level(0) # AUTOCOMMIT
    return dbConnection

