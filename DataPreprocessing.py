## Install all the required packages and run the required libraries
##-----------------------------------------------
import pandas as pd
import numpy as np
from skimpy import clean_columns
import re

## List of Functions
##-----------------------------------------------
#write to CSV file
#writeCSV(table DataSet, String file):The writeCSV function writes the dataframe to a CSV file.
#Arguments: DataSet is the name of dataframe to save, file is the name of the CSV output file.
#Returns a CSV file
def writeCSV(dataset,filename):
     dataset.to_csv(filename)

#read a CSV file
#readCSV(String file):The readCSV function reads the CSV file into workplace
#Arguments: file is the CSV file in the select work directory.
# def readCSV(filename,time,format="%d/%m/%Y %H:%M"):
def readCSV(filename,time):
    csv_data = pd.read_csv(filename)
    cleanAllHeaders(csv_data)
    csv_data=addNewTime(csv_data,cleanText(time),"%d/%m/%Y %H:%M")
    return csv_data

def cleanText(colName):
    cleanedText=re.sub("[$&+,:;=?@#|'<>.^*()%!-]","_",colName).lower()
    return cleanedText;

#select dataset columns for analysis
#selectColumns(table DataSet, string columnName, …): This function selects/keeps the list columns needed for analysis from the dataset. Only the
#list of selected columns/attributes are included in the dataset. 
#Arguments: DataSet is the name of the dataframe, columnName is the name of the column to keep in the dataset. Many can be listed, separated by commas.
#Returns a dataset including only the list of columns/attributes that are selected
def selectColumns(dataset,selectCol):
    return dataset[selectCol]

#delete columns from the dataset
def deleteColumns(dataset,deleteCol):
    return dataset.drop(columns=deleteCol)

def cleanOneHeader(dataset,col_name):
    cleaned=cleanText(col_name)
    dataset=dataset.rename(columns={col_name:cleaned})
    return dataset;

#clean column headers
#cleanHeaders(table DataSet)
#This function cleans the headers of the columns from spaces and other special characters.
#It only keeps lower case letters, numbers, and underscores (_). The spaces are replaced by ‘_’ and the special characters are removed. 
#Returns a dataframe with clean header names
def cleanAllHeaders(dataset):
    col_list=dataset.columns.tolist()
    for i in col_list:
        cleaned=cleanText(i)
        dataset=dataset.rename(columns={i:cleaned})
    return dataset;

#filter rows
#The filter function keeps records/rows based on the conditions specified. 
#Only the rows where the condition is TRUE are kept in the DataSet. 
#The filter function supports multiple functions, for example: ==, >, <, >=, <=, &, | , ! . 
def filterRows(dataset,conditions):
    return dataset.query(conditions)

##remove rows with low frequency
#Column represents the column you want to filter
# freq the threshold value that is used to filter out rows whose count is less than freq.
def removeEventsLowFrequency(dataset,event,freq):
    return dataset[dataset.groupby(event)[event].transform('count').gt(freq)]

#delete traces with number of events less than a specific number (num)
def deleteTraceLengthLessThan(dataset,company_id,num):
    return dataset.groupby(company_id).filter(lambda x : len(x)>=num)

# delete traces that do not start with one of many start events
# Not Sort 
def deleteTruncatedTracesStart(dataset,company_id,event,start_events):
    return data.groupby(company_id).filter(lambda oneCompanyData: oneCompanyData.iloc[0][event] in start_events)

## multiple conditions, values

# delete traces that do not start with one of many start events
# Need to Sort 
def deleteTruncatedTracesStartSort(dataset,company_id,event,start_events):
    dataset.sort_values(by=[company_id,'new_time'])
    return data.groupby(company_id).filter(lambda oneCompanyData: oneCompanyData.iloc[0][event] in start_events)

#delete traces that do not end with one of many end events
# Need to sort

def deleteTruncatedTracesEndSort(dataset,company_id,event,end_events):
    dataset.sort_values(by=[company_id,'new_time'])
    return deleteTruncatedTracesEnd(dataset,company_id,event,end_events)
# multiple conditions, values

# delete traces that do not end with a specific end event
# No need to sort

def deleteTruncatedTracesEnd(dataset,company_id,event,end_events):
    return data.groupby(company_id).filter(lambda oneCompanyData: oneCompanyData.iloc[-1][event] in end_events)

#delete traces with total duration less than t
def deleteTracesWithTimeLessSort(dataset,company_id,t):
    dataset=dataset.sort_values(by=[company_id,'new_time'])
    result=dataset.groupby(company_id).filter(lambda oneCompanyData: (oneCompanyData.iloc[-1].new_time - oneCompanyData.iloc[0].new_time) > t)
    return result

## format condition

# Change the format of time
def addNewTime(dataset,time,formats):
    dataset['new_time']=pd.to_datetime(dataset[time],format=formats)
    dataset['new_time']=(dataset['new_time'].dt.hour*60+dataset['new_time'].dt.minute)*60 + dataset['new_time'].dt.second
    return dataset

#concatenate two columns

# data["test"] = data["event"].astype(str) + data["theme"].astype(str)
# Add separator

def concatenateColumns(dataset,newCol,separator,*cols):
    dataset[newCol]=""
    for col in cols:
        dataset[newCol]=dataset[newCol]+separator+dataset[col].astype(str)
    return  dataset

# In this example, the keep parameter is set to False, so that only Unique values are taken and the duplicate values are removed from data.
# Determines which duplicates (if any) to mark.
# first : Mark duplicates as True except for the first occurrence.
# last : Mark duplicates as True except for the last occurrence.
# False : Mark all duplicates as True.
## Group by id
def eventIsRepeated(dataset,company_id,event):
    res = pd.DataFrame([])
    grouped = dataset.groupby(company_id)
    for name, group in grouped:
        group['match1'] = group.event.eq(group.event.shift()) 
        group['match2'] = group.event.eq(group.event.shift(-1)) 
        group["isRepeated"] = group.apply(lambda x: x['match1'] or x['match2'], axis = 1)
        group = group.drop(["match1", "match2"], axis = 1)
        res = pd.concat([res, group])
    return res

# In this example, the keep parameter is set to False, so that only Unique values are taken and the duplicate values are removed from data.
# Determines which duplicates (if any) to mark.
# first : Mark duplicates as True except for the first occurrence.
# last : Mark duplicates as True except for the last occurrence.
# False : Mark all duplicates as True.
## Group by id
def eventIsRepeatedSort(dataset,company_id,event):
    dataset.sort_values(by=[company_id,'new_time'])
    res = pd.DataFrame([])
    grouped = dataset.groupby(company_id)
    for name, group in grouped:
        group['match1'] = group.event.eq(group.event.shift()) 
        group['match2'] = group.event.eq(group.event.shift(-1)) 
        group["isRepeated"] = group.apply(lambda x: x['match1'] or x['match2'], axis = 1)
        group = group.drop(["match1", "match2"], axis = 1)
        res = pd.concat([res, group])
    return res

# keep first event in each sequence of consecutive events in each trace of logs
def keepFirstEventSort(dataset,event,company_id):
    dataset=dataset.sort_values(by=[company_id,'new_time'])
    res = pd.DataFrame([])
    grouped2 = dataset.groupby(company_id)
    for name, group in grouped2:
        test = group.loc[group[event].ne(group[event].shift())]
        res = pd.concat([res, test])
    return res

# keep first event in each sequence of consecutive events in each trace of logs
def keepFirstEvent(dataset,event,company_id):
    res = pd.DataFrame([])
    grouped2 = dataset.groupby(company_id)
    for name, group in grouped2:
        test = group.loc[group[event].ne(group[event].shift())]
        res = pd.concat([res, test])
    return res

# keep last event in each sequence of consecutive events in each trace of logs
def keepLastEventSort(dataset,event,company_id):
    dataset=dataset.sort_values(by=[company_id,'new_time'],ascending=False)
    res = pd.DataFrame([])
    grouped2 = dataset.groupby(company_id)
    for name, group in grouped2:
        test = group.loc[group[event].ne(group[event].shift())]
        res = pd.concat([res, test])
    return res.sort_values(by=[company_id,'new_time'])

# keep last event in each sequence of consecutive events in each trace of logs
def keepLastEvent(dataset,event,company_id,time):
    res = pd.DataFrame([])
    grouped2 = dataset.groupby(company_id)
    for name, group in grouped2:
        test = group.loc[group[event].ne(group[event].shift())]
        res = pd.concat([res, test])
    return res.sort_values(by=[company_id,time])

#delete all events
def deleteAllEvents(dataset,events,eventName):
    return dataset.drop(dataset[dataset[events]==eventName].index)

#Merge rows no sort
def MergeSameEventRows(dataset,company_id,event_name,conditions):
    res = pd.DataFrame([])
    grouped = dataset.groupby(company_id)
    for name, group in grouped:
        group['sup'] = group.event.eq(group.event.shift()).map(lambda x: 0 if x == True else 1 ).cumsum()
        res = pd.concat([res, group])
    
    lists = data.columns.to_list()
    ignore = [company_id]
    for column in lists:
        if column not in ignore and column not in conditions:
            conditions[column] = "first"
    test = res.groupby([company_id, "sup"]).agg(conditions).reset_index().drop(["sup"], axis=1)
    return test
    
#Merge rows sort
def MergeSameEventRowsSort(dataset,company_id,event_name,conditions):
    dataset.sort_values(by=[company_id,'new_time'])
    res = pd.DataFrame([])
    grouped = dataset.groupby(company_id)
    for name, group in grouped:
        group['sup'] = group.event.eq(group.event.shift()).map(lambda x: 0 if x == True else 1 ).cumsum()
        res = pd.concat([res, group])
    
    lists = data.columns.to_list()
    ignore = [company_id]
    for column in lists:
        if column not in ignore and column not in conditions:
            conditions[column] = "first"
    result = res.groupby([company_id, "sup"]).agg(conditions).reset_index().drop(["sup"], axis=1)
    return result
    
def ArrangeRows(dataset,condition):
    return data.sort_values(by=condition)