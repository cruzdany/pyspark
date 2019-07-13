#!/usr/bin/env python3

#BY DANIEL GIORDANNO MAGAÑA CRUZ
#These are the libraries that are really needed for in order to make this whole task

import databricks.koalas as ks
import pandas as pd
import numpy as np
import datetime
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from datetime import datetime
import collections as cl
import os

sc = SparkContext.getOrCreate(); #Here, we are starting spark functions

#We open up all the files
w1=pd.read_csv("weather01.txt", sep ='\s+')
w2=pd.read_csv("weather02.txt", sep ='\s+')
w3=pd.read_csv("weather03.txt", sep ='\s+')
w4=pd.read_csv("weather04.txt", sep ='\s+')
w5=pd.read_csv("weather05.txt", sep ='\s+')
w6=pd.read_csv("weather06.txt", sep ='\s+')

'''Since I had already worked with this, I only want to check the columns
names of this variable'''

w1.columns

#Here, I rename those columns

w1=w1.rename(columns={'X':'Number', 'year':'Year', 'month':'Month', 'measure':'Measure'})

#We can see now, that they have already changed

w1.columns

#Now, we proceed to merge all the files in a single one

weather=pd.concat([w1,w2,w3,w4,w5,w6], axis=1)

#We visualise it in order to see if everythig is just fine
weather.head(3)

#We can see that there is a columns that is not useful for us and we proceed to delete it

del(weather['Number'])

#We visualise it in order to see if everythig is just fine again

weather.head(3)

#We proceed to make the  make the melting and creating a new column named value

weather = pd.melt(weather,
                  id_vars=['Year', 'Month', 'Measure'],
                  var_name='Day');

#We delete missing values
weather.dropna(inplace=True)

#In order to rename columns, we have to reset the index
weather = weather.reset_index(drop=True)

#Now, we change the name of the column value to Value
weather=weather.rename(columns={'value':'Value'})

#We create a function in order that the date shows up like YYYY-MM-DD

def creating_date(row):
    return "%d-%02d-%02d" % (row['Year'], row['Month'], int(row['Day'][1:]))

#We create a new column named Date and inside of it we apply the function
weather['Date'] = weather.apply(creating_date,axis=1)

#We choose the colums we want to use
weather = weather [['Measure', 'Value', 'Date']]

#This deletes the missing values
#Here we are using the numpy library in order to substitute the values and then delete them
weather = weather.replace('<NA>',np.NaN)
weather = weather.dropna()

#Here, we can see that those missing values are not anymore therew.
weather.tail()

#Here we restruct the set by using pivot

weather = weather.pivot(index='Date', columns= 'Measure', values='Value')

#We visualize it in order to see that everyting has changed

weather.head(3)

#We rest the index again
weather = weather.reset_index()

#Here, whis only will take in consideration the values in a range of 365 because of the days in a year
weather = weather.drop(365,axis=0)

#We visualize the column events 
weather['Events']

#We substitute the mising values with others that are random by using the method "ffill"
weather['Events']=weather['Events'].fillna(method='ffill')

#We proceed to sort the values from the Date column
weather = weather.sort_values('Date',ascending= False)
#We reset the index as Date 
weather = weather.set_index('Date',drop=True)
#We convert the index in the datetime way
weather.index = pd.to_datetime(weather.index)

#Now, we check the whole set by knowing its info
weather.info()

#Now we change the type of data of those columns
weather['Max.TemperatureF'] = weather['Max.TemperatureF'].astype(int)
weather['Min.TemperatureF'] = weather['Min.TemperatureF'].astype(int)
weather['Mean.TemperatureF'] = weather['Mean.TemperatureF'].astype(int)
weather['Max.Dew.PointF'] = weather['Max.Dew.PointF'].astype(int)
weather['MeanDew.PointF'] = weather['MeanDew.PointF'].astype(int)
weather['Min.DewpointF'] = weather['Min.DewpointF'].astype(int)

#Now, we convert it into a Koalas Dataframe
koala1 = ks.from_pandas(weather)
#Here, we can check what the weather is, it should show up like a DataFrame
type(koala1)
#This should show us up: "databricks.koalas.frame.DataFrame"

#Now we check if everything is fine 
koala1.head(5)

#Here, we are converting it to a csv file 
weather.to_csv('weathercsv_outputfile.csv')

#Here, we close our pyspark session or work
sc.stop()
