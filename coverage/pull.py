#!/usr/bin/python

import os
import sys
import requests
import time
from bs4 import BeautifulSoup
from requests.auth import HTTPBasicAuth
from requests.exceptions import Timeout
from config import *

print("Get LP Daac index file")
indexLocal = workingDir + "/" + os.path.basename('index.html')
if os.path.exists(indexLocal):
    print('File exists: ' + indexLocal)
else:
    print("Fetching index, please wait")
    response=requests.get(baseUrl)
    with open(indexLocal, mode='wb') as localfile:     
        localfile.write(response.content)
    print('Fetch complete - file stored at ' + indexLocal)

def fetch(filename):
    localFilename = workingDir + "/" + os.path.basename(filename)
    remoteFilename = baseUrl + "/" + os.path.basename(filename)
    if os.path.exists(localFilename):
        print('File exists: ' + localFilename)
        return None
    else:
        print("Fetch",remoteFilename)
        try:
            response=requests.get(remoteFilename, timeout=(2,5))
            with open(localFilename, mode='wb') as localfile:     
                localfile.write(response.content)

        except KeyboardInterrupt:
            print('** Stopped **')
            sys.exit()
        except Timeout:
          print('Timeout')
          time.sleep(60)

    return localFilename
    
def fetchAll():
    success = False
    with open(indexLocal, mode='r') as localfile: 
        soup = BeautifulSoup(localfile.read(), features="lxml")
        for link in soup.find_all('a'):
            l=link.get('href')
            if (l.endswith(".1.jpg") or l.endswith(".zip.xml")):
                if (fetch(l) != None):
                   success = True
    return success

while True:
  try:
    if (fetchAll() != True):
      print("All files downloaded")
      break
  except Exception as e:
    print("Ouch!", e);
    time.sleep(600)
