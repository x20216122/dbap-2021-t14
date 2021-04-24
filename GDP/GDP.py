#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#This code reads in file, converts to json, investigates missing data, 
#adds missing data and stores the new data into a azure cosmos mongo db


# In[1]:


import numpy as np
import pandas as pd
import json
import math
import pymongo

print('Imported packages successfully.')


# In[2]:


#Function to Open either a csv file or json file
def fileOpen(data):
    if data.endswith('.csv'):
        try:
            result = pd.read_csv(data)
            print('Success')
            return result
        except:
            print("Something went wrong, please check your code")
    elif data.endswith('.json'):
        try:
            with open(data) as f:
                data = json.load(f)
                print('Success')
                return(data)
        except: 
            print("Something went wrong, please check your code")   
    else:
        print('File is not CSV or JSON')


# In[5]:


data = fileOpen('data/world-gdp.csv')


# In[6]:


#convert data to Json
data.to_json (r'data/world-gdp.json')


# In[7]:


gdp = fileOpen('data/world-gdp.json')


# In[8]:


#Create Pandas Dataframe
df = pd.DataFrame(gdp)


# In[9]:


#Check for Missing Values
df.isnull().any().any()


# In[10]:


#Frequency of Missing Values
df.isna().sum().sum()


# In[11]:


#Determing what columns are missing values
df.isna().sum()/(len(df))*100


# In[12]:


#Determining the percentage of the columns with the least missing values and the most missing values
minmax = df.isna().sum()/(len(df))*100
print("The column with lowest amount of missing values is missing {} % of its values.".format(minmax.min()))
print("The column with highest amount of missing values is missing {} % of its values.".format(minmax.max()))


# In[13]:


#Showing all columns missing at least one value
df.loc[:, df.isnull().any()].columns


# In[14]:


#Function to return the countries missing a gdp value for a particular year (Pass in the year as str)
def missingGDP(year):
    try:
        i = -1
        missing = []
        item = float('nan')
        for item in df[year]:
            i = i + 1
            if math.isnan(item) == True:
                missing.append("Index: "+str(i)+" Country Code & Name: "+df['Country Code'][i]+" "+df['Country Name'][i])
        return(missing)
    except: 
        print ("Sorry Something has gone wrong")


# In[15]:


#Call the function (We are particularly interested in 2015)
missingGDP('2015')


# In[16]:


#Venuzela stopped reporting GDP in 2014, so we will use their 2014 GDP for 2015.
df['2015'][252] = df['2014'][252]
print(df['2015'][252])

#I found the GDP for some of the other countries:

#Somalias GDP (https://tradingeconomics.com/somalia/gdp)
df['2015'][211] = 6670000000

#Eritrea GDP (https://tradingeconomics.com/eritrea/gdp)
df['2015'][67] = 4440000000

#Syria GDP (https://www.statista.com/statistics/742532/gdp-in-syria/#:~:text=From%20the%20last%20known%20measure,in%20economic%20output%20from%202008.)
df['2015'][225] = 14000000000

#The rest of the data are from overseas territories and North Korea (Hey Rocketman!) so we'll leave them as is.


# In[17]:


#Checking the new values are no longer missing.
missingGDP('2015')


# In[18]:


#converting df to a dict for insertion into mongodb
gdpdict = df.to_dict()


# In[20]:


#Inserting data into the cosmos mongo db

myclient = pymongo.MongoClient("mongodb://x20216211:b60dpNcz9xCgM2W7BghYHP8M6XBFuDszuSSKqM2kb03m4TSEhN1zcfIkP1z8XVg6tNW0szA5S6HChOWjfoIMrw==@x20216211.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@x20216211@")

mydb = myclient["gdp"]

for key, value in gdpdict.items():
    mycol = mydb[key]
    mycol.insert_one(value)
    


# In[ ]:




