#!/usr/bin/python
import psycopg2
from os import listdir
import pymongo
import xmltodict

workingDir = '/var/data/david.mcnerney/Project/'

def getCollection():
    uri = "mongodb://x20216122:MpvRuzyAEjPCDmPJEq4uMvdV3K0cxUJX5IqNEfbbUqVfeGnjz2tPCEBplQUFAbg3VEzXJZnyQwMAnP1MbBykDQ==@x20216122.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@x20216122@"
    client = pymongo.MongoClient(uri)
    db = client['lp-daac']
    return db['indices']

def connect(db="x20216122"):
    dbConnection = psycopg2.connect(
        user = "postgres",
        password = "postgres",
        host = "192.168.0.24",
        port = "5432",
        database = db)
    dbConnection.set_isolation_level(0) # AUTOCOMMIT
    return dbConnection

def syncIndexFiles():
  xmlFiles = []

  for f in listdir(workingDir):
      if (f.endswith('.xml')):
        xmlFiles.append(f)

  collection = getCollection()

  for d in collection.find({},{'file':1}):
      if ('file' in d):
          if (d['file'] in xmlFiles):
              xmlFiles.remove(d['file'])

  for xmlFile in xmlFiles:
      path=workingDir + xmlFile
      print(path)
      with open(path) as f:
        json = {'file':xmlFile,'data':xmltodict.parse(f.read())}
        collection.insert_one(json)
        
def syncMapFiles():
    dbConnection = connect()
    try:
        dbCursor = dbConnection.cursor()
        dbCursor.execute("select filename from mappingfile")
        trackedFiles = [r[0] for r in dbCursor.fetchall()]

        dbCursor.close()
    except e:
        print("Something went wrong",e )

    collection = getCollection()
    
    dbCursor = dbConnection.cursor()

    for d in collection.find():
        f = d['data']['GranuleMetaDataFile']['GranuleURMetaData']['DataFiles']\
            ['DataFileContainer']['DistributedFileName']
        if (f not in trackedFiles):
            print(f)
            dbCursor.execute("INSERT INTO mappingfile (filename) VALUES ('{}')".format(f))

    dbCursor.close()

syncIndexFiles()
syncMapFiles()
