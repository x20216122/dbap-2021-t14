#!/usr/bin/python
import psycopg2
from os import listdir
import pymongo
import xmltodict
from config import *

def syncIndexFiles():
  print("Syncing index files")
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
    print("Syncing map files")
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
