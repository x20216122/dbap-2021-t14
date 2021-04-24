#!/usr/bin/python

import json
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
import os
import sys
import psycopg2
import pymongo
import collections
import time

import requests
import zipfile
import tempfile
import re
import numpy
from os import listdir
from PIL import Image
from config import *

def initPolygons():
    polygons = []
    with open('world-administrative-boundaries-countries.json') as fp:
        countries = json.load(fp)
        for country in countries:
            coordinates = country['fields']['geo_shape']['coordinates']
            for p0 in coordinates:
                try:
                    if (country['fields']['geo_shape']['type'] == 'Polygon'):
                        polygons.append({'id':country['fields']['iso3_code'],
                            'c':country['fields']['preferred_term'], 'p':p0, 'm':centroid(p0)})
                    else:
                        for p1 in p0:
                            polygons.append({'id':country['fields']['iso3_code'],
                                  'c':country['fields']['preferred_term'], \
                                  'p':p1, 'm':centroid(p1)})
                except e:
                    print("Error:", country['fields']['preferred_term'], e)

    for p in polygons:
        p['pg'] = Polygon(p['p'])
        p['bb'] = p['pg'].bounds

    return polygons

def coordToCountry(point, polygons):
    for p in polygons:
        polygon = p['pg']
        if polygon.contains(point):
            return p

def countriesInQuad(quad, polygons):
    result = []
    qp = Polygon(quad)
    for p in polygons:
        polygon = p['pg']
        if (qp.overlaps(polygon) or qp.contains(polygon) or qp.within(polygon)):
            result.append(p)
    return result

def centroid(polygon):
    l = len(polygon)
    x = sum([p[0] for p in polygon])
    y = sum([p[1] for p in polygon])
    return (x/l,y/l)

# https://math.stackexchange.com/questions/2007116/quadrilateral-interpolation
def mapPoint(q, pt):
    def mapPoint1(pt, q1, q2, q3, q4):
        px = pt[0]*2.0-1.0
        py = pt[1]*2.0-1.0
        A=0.25*( q1 + q2 + q3 + q4)
        B=0.25*(-q1 + q2 - q3 + q4)
        C=0.25*(-q1 - q2 + q3 + q4)
        D=0.25*( q1 - q2 - q3 + q4)
        return A + (B*px) + (C*py) + (D*px*py)

    return (mapPoint1(pt, q[0][0], q[1][0], q[3][0], q[2][0]),
            mapPoint1(pt, q[0][1], q[1][1], q[3][1], q[2][1]))

def reduce(arr, step):
    return arr[round(step/2)::step,round(step/2)::step]

def saveMapping(indexFile, result, status):
    # Save results
    dbConnection = connect()
    dbCursor = dbConnection.cursor()

    dbCursor.execute("SELECT id FROM mappingfile WHERE filename='{}'".format(indexFile))
    r0 = dbCursor.fetchmany(1)
    if (not r0):
        return

    mappingFileId = r0[0][0]

    print("Save Mapping", indexFile, mappingFileId, len(result), set([r[0] for r in result.elements()]))

    dbCursor.execute("DELETE FROM mapping WHERE mappingfile_id = {}".format(mappingFileId))
    for r in result:
        dbCursor.execute("INSERT INTO mapping (mappingfile_id, country_code, value, counter) \
                         VALUES ({}, '{}', {}, {})".format(mappingFileId, r[0], r[1], result[r]))

    dbCursor.execute("UPDATE mappingfile SET status={} WHERE id={}".format(status, mappingFileId))
    dbConnection.commit()
    dbCursor.close()


def getCountry(z, countries, dd, quad):
    ((y,x),v) = z
    if (v==255 or v==0):
        return None
    p = mapPoint(quad, (x/dd[0], y/dd[1]))
    return coordToCountry(Point(p[0],p[1]), countries)

# Process next task
def getNextTask():
    dbConnection = connect()
    try:
        dbCursor = dbConnection.cursor()
        dbCursor.execute("select filename from mappingfile where status < 5 order by status desc limit 1")
        rows = dbCursor.fetchmany(1)

        dbCursor.close()
    except e:
        print("Something went wrong",e )
    if (not rows):
        return None
    zipfile = rows[0][0]
    

def taskToArray(file):

    if os.path.exists(file + "XXX"):  # if the tif file exists prioritize it
      tempDir = tempfile.TemporaryDirectory(dir = workingDir)
      print("Extracting", file, tempDir.name)

      with zipfile.ZipFile(file, 'r') as zip_ref:
          fs=zip_ref.extractall(tempDir.name)
          
      for f in listdir(tempDir.name):
          if f.endswith('.tif') and not f.endswith('err.tif'):
              tifFile = tempDir.name + "/" + f
              im = Image.open(tifFile)
              #pal=im.getpalette()
              #print([(pal[i] + pal[i+1] + pal[i+2])/3 for i in range(0,330,3)])
              return (tifFile, reduce(numpy.array(im), 10))
    else:
      jpgFile = file[:-4] + ".1.jpg"
      if os.path.exists(jpgFile):
        im = Image.open(jpgFile).convert('L')
        return (jpgFile, numpy.asarray(im))
        

    return None

def bounce(list, item):
    ix = list.index(item)
    if (ix != 0):
        list.insert(0,list.pop(ix))
  
    
def processTask(task):
    ix = None
    for d in getCollection().find({'file':task + '.xml'}):
        ix = d['data']['GranuleMetaDataFile']['GranuleURMetaData']['SpatialDomainContainer']\
             ['HorizontalSpatialDomainContainer']['GPolygon']['Boundary']['Point']
        break
        
    if (ix == None):
        return None

    quad = [(float(x['PointLongitude']),float(x['PointLatitude'])) for x in ix]
    (filename, imarray) = taskToArray(workingDir + "/" + task)
    trf = filename.endswith(".jpg") # perform small transformation on jpg values
    dd = numpy.shape(imarray)[::-1]
    countries = list(countryData)
    likelyCountries = countriesInQuad(quad, countryData)
    for c in likelyCountries:
      bounce(countries, c)
    
    print("Processing:", filename, dd, [c['id'] for c in likelyCountries])

    result = collections.Counter()
        
    ct=0
    for z in numpy.ndenumerate(imarray):
        v = z[1]
        if (trf):
          if (v < 18):
            continue
          v = round((255-v)*99/237)+1
        ct+=1
        if (ct % 50000 == 0): 
          print('.', end='', flush=True)
          time.sleep(5)
        c = getCountry(z, countries, dd, quad)
        if (c != None):
            bounce(countries, c)
            result[(c['id'],v)] += 1
            
    print("")
    saveMapping(task, result, 9 if trf else 8)
    
countryData = initPolygons()
#while True:
#  nextTask = getNextTask()
#  if (nextTask == None):
#    break;
#  processTask(nextTask)

def err(x):
  print("ERr", x)

import multiprocessing as mp

pool = mp.Pool(10)

dbConnection = connect()
try:
    dbCursor = dbConnection.cursor()
    dbCursor.execute("select filename from mappingfile where status < 5 order by status desc")
    rows = [row[0] for row in dbCursor.fetchall()]

    dbCursor.close()
except e:
    print("Something went wrong",e )

for nextTask in rows:
  print("Task", nextTask)
  pool.apply_async(processTask, args=(nextTask,), error_callback=err)

pool.close()
pool.join()
