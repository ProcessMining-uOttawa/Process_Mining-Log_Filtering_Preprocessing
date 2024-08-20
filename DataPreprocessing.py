## Install all the required packages and run the required libraries
##-----------------------------------------------
import pandas as pd
import numpy as np
import warnings
from skimpy import clean_columns
from datetime import datetime as dt
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
def readCSV(filename,case_id_col_name="id",event_col_name="event",timestamp_col_name="timestamp",format="%d/%m/%Y %H:%M"): #added by JT
    csv_data = pd.read_csv(filename)
    csv_data = cleanAllHeaders(csv_data,case_id_col_name,event_col_name,timestamp_col_name)
    csv_data = addNewTime(csv_data,cleanText(timestamp_col_name),format)
    return csv_data

def readExcel(filename,case_id_col_name="id",event_col_name="event",timestamp_col_name="timestamp",format="%d/%m/%Y %H:%M"): #added by JT
    xls_data = pd.read_excel(filename) 
    xls_data = cleanAllHeaders(xls_data,case_id_col_name,event_col_name,timestamp_col_name)
    xls_data=addNewTime(xls_data,cleanText(timestamp_col_name),format)
    return xls_data

def readPanda(panda,case_id_col_name="id",event_col_name="event",timestamp_col_name="timestamp",format="%d/%m/%Y %H:%M"): #added by JT
    pd_data = panda
    pd_data = cleanAllHeaders(pd_data,case_id_col_name,event_col_name,timestamp_col_name)
    pd_data=addNewTime(pd_data,cleanText(timestamp_col_name),format)
    return pd_data


# Added by JT
# case_id: specifies what case id column to use
# time: specifies what timestamp column to use
# events: { event_name : timestampColumnToUse }
# used when there are single rows with multiple timestamp columns that
# can be broken down into multiple (distinct) events
def transposeColumnsToEventLog(dataset,events,case_id_col_name="id",format="%d/%m/%Y %H:%M",resourcecol=False):
    dataset = ArrangeRows(dataset,[case_id_col_name])
    event_log = []
    for index,row in dataset.iterrows():
        for key, value in events.items(): 
            if resourcecol != False:
                event_log.append({
                       'case_id'   : row[case_id_col_name],
                       'event'     : key,
                       'timestamp' : row[value],
                       'resource'  : row[resourcecol],
                        })    
            else:
                event_log.append({
                    'case_id' : row[case_id_col_name],
                      'event' : key,
                  'timestamp' : row[value],
                })

    return readPanda(pd.DataFrame(event_log),case_id_col_name,'event','timestamp',format)


def cleanText(colName):
    cleanedText=re.sub("[$&+,:;=?@#|'<>.^*()%!-]","_",colName).lower()
    return cleanedText

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
    return dataset


#clean column headers
#cleanHeaders(table DataSet)
#This function cleans the headers of the columns from spaces and other special characters.
#It only keeps lower case letters, numbers, and underscores (_). The spaces are replaced by ‘_’ and the special characters are removed. 
#Returns a dataframe with clean header names
def cleanAllHeaders(dataset,case_id_col_name,event_col_name,timestamp_col_name):
    dataset=dataset.rename(columns={case_id_col_name:'case_id',event_col_name:'event',timestamp_col_name:'timestamp'})
    col_list=dataset.columns.tolist()
    for i in col_list:
        cleaned=cleanText(i)
        dataset=dataset.rename(columns={i:cleaned})
    return dataset


#filter rows
#The filter function keeps records/rows based on the conditions specified. 
#Only the rows where the condition is TRUE are kept in the DataSet. 
#The filter function supports multiple functions, for example: ==, >, <, >=, <=, &, | , ! . 
def filterRows(dataset,conditions):
    return dataset.query(conditions)


##remove rows with low frequency
#Column represents the column you want to filter
# freq the threshold value that is used to filter out rows whose count is less than freq.
def removeEventsLowFrequency(dataset,freq,exceptions=None):
    if exceptions is None:
        return dataset[dataset.groupby('event')['event'].transform('count').gt(freq)]
    
    if type(exceptions) is str:
        dataset_e = dataset[dataset['event'] == exceptions]
        if len(dataset_e) > 0 and dataset_e['event'].value_counts()[0] > freq:
            warnings.warn("Warning: Your exception event(s) occurs more often than your cut off. This will lead to duplicate occurences of your exceptions in the event log.")

        dataset_f = dataset[dataset.groupby('event')['event'].transform('count').gt(freq)]
        dataset = pd.concat([dataset_e,dataset_f])
        return ArrangeRows(dataset,['case_id','timestamp'])

    if type(exceptions) is list:
        dataset_e = dataset[dataset['event'].isin(exceptions)]        
        dataset_f = dataset_f = dataset[dataset.groupby('event')['event'].transform('count').gt(freq)]
        dataset = pd.concat(dataset_e,dataset_f)
        return ArrangeRows(dataset,['case_id','timestamp'])

    return False


#delete traces with number of events less than a specific number (num)
def deleteTraceLengthLessThan(dataset,num):
    return dataset.groupby('case_id').filter(lambda x : len(x)>=num)


# delete traces that do not start with one of many start events
# Not Sort 
def deleteTruncatedTracesStart(dataset,start_events):
    return dataset.groupby('case_id').filter(lambda oneCompanyData: oneCompanyData.iloc[0]['event'] in start_events)

## multiple conditions, values


# delete traces that do not start with one of many start events
# Need to Sort 
def deleteTruncatedTracesStartSort(dataset,start_events):
    dataset.sort_values(by=['case_id','new_time'])
    return dataset.groupby('case_id').filter(lambda oneCompanyData: oneCompanyData.iloc[0]['event'] in start_events)


#delete traces that do not end with one of many end events
# Need to sort

def deleteTruncatedTracesEndSort(dataset,end_events):
    dataset.sort_values(by=['case_id','new_time'])
    return deleteTruncatedTracesEnd(dataset,'case_id','event',end_events)
# multiple conditions, values


# delete traces that do not end with a specific end event
# No need to sort
def deleteTruncatedTracesEnd(dataset,end_events):
    return dataset.groupby('case_id').filter(lambda oneCompanyData: oneCompanyData.iloc[-1]['event'] in end_events)


#delete traces with total duration less than t
def deleteTracesWithTimeLessSort(dataset,t):
    dataset=dataset.sort_values(by=['case_id','new_time'])
    result=dataset.groupby('case_id').filter(lambda oneCompanyData: (oneCompanyData.iloc[-1].new_time - oneCompanyData.iloc[0].new_time) > t)
    return result

## format condition


#delete traces with total duration less than t
# Without sorting
def deleteTracesWithTimeLessSort(dataset,t):
    result=dataset.groupby('case_id').filter(lambda oneCompanyData: (oneCompanyData.iloc[-1].new_time - oneCompanyData.iloc[0].new_time) > t)
    return result


# Change the format of time
def addNewTime(dataset,time,formats="%d/%m/%Y %H:%M"):
    dates=pd.to_datetime(dataset[time],format=formats)
    dataset['new_time']=(dates - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')
    return dataset


#concatenate two columns
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
def eventIsRepeated(dataset):
    res = pd.DataFrame([])
    grouped = dataset.groupby('case_id')
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
def eventIsRepeatedSort(dataset):
    dataset.sort_values(by=['case_id','new_time'])
    res = pd.DataFrame([])
    grouped = dataset.groupby('case_id')
    for name, group in grouped:
        group['match1'] = group.event.eq(group.event.shift()) 
        group['match2'] = group.event.eq(group.event.shift(-1)) 
        group["isRepeated"] = group.apply(lambda x: x['match1'] or x['match2'], axis = 1)
        group = group.drop(["match1", "match2"], axis = 1)
        res = pd.concat([res, group])
    return res


# keep first event in each sequence of consecutive events in each trace of logs
def keepFirstEventSort(dataset):
    dataset=dataset.sort_values(by=['case_id','new_time'])
    res = pd.DataFrame([])
    grouped2 = dataset.groupby('case_id')
    for name, group in grouped2:
        test = group.loc[group['event'].ne(group['event'].shift())]
        res = pd.concat([res, test])
    return res


# keep first event in each sequence of consecutive events in each trace of logs
def keepFirstEvent(dataset):
    res = pd.DataFrame([])
    grouped2 = dataset.groupby('case_id')
    for name, group in grouped2:
        test = group.loc[group['event'].ne(group['event'].shift())]
        res = pd.concat([res, test])
    return res


# keep last event in each sequence of consecutive events in each trace of logs
def keepLastEventSort(dataset):
    dataset=dataset.sort_values(by=['case_id','new_time'],ascending=False)
    res = pd.DataFrame([])
    grouped2 = dataset.groupby('case_id')
    for name, group in grouped2:
        test = group.loc[group['event'].ne(group['event'].shift())]
        res = pd.concat([res, test])
    return res.sort_values(by=['case_id','new_time'])


# keep last event in each sequence of consecutive events in each trace of logs
def keepLastEvent(dataset,time):
    res = pd.DataFrame([])
    grouped2 = dataset.groupby('case_id')
    for name, group in grouped2:
        test = group.loc[group['event'].ne(group['event'].shift())]
        res = pd.concat([res, test])
    return res.sort_values(by=['case_id',time])


#delete all events
def deleteAllEvents(dataset,eventNames):
  if type(eventNames) is list:
    for eventName in eventNames:
        dataset = dataset.groupby('case_id').filter(lambda g: (g.event != eventName).all())
    return dataset
  
  return dataset.groupby('case_id').filter(lambda g: (g.event != eventNames).all())


#Merge rows no sort
def MergeSameEventRows(dataset,conditions):
    res = pd.DataFrame([])
    grouped = dataset.groupby('case_id')
    for name, group in grouped:
        group['sup'] = group.event.eq(group.event.shift()).map(lambda x: 0 if x == True else 1 ).cumsum()
        res = pd.concat([res, group])
    
    lists = dataset.columns.to_list()
    ignore = ['case_id']
    for column in lists:
        if column not in ignore and column not in conditions:
            conditions[column] = "first"
    test = res.groupby(['case_id', 'sup']).agg(conditions).reset_index().drop(['sup'], axis=1)
    return test
    

#Merge rows sort
def MergeSameEventRowsSort(dataset,conditions):
    dataset.sort_values(by=['case_id','new_time'])
    res = pd.DataFrame([])
    grouped = dataset.groupby('case_id')
    for name, group in grouped:
        group['sup'] = group.event.eq(group.event.shift()).map(lambda x: 0 if x == True else 1 ).cumsum()
        res = pd.concat([res, group])
    
    lists = dataset.columns.to_list()
    ignore = ['case_id']
    for column in lists:
        if column not in ignore and column not in conditions:
            conditions[column] = "first"
    result = res.groupby(['case_id', 'sup']).agg(conditions).reset_index().drop(['sup'], axis=1)
    return result
    

#Modified by JT
def ArrangeRows(dataset,condition,ascending=True):
    return dataset.sort_values(by=condition,ascending=ascending) # added by JT

# Added by JT
# This function deletes duplicate events that occur within a very close time of eachother
# You can specify the time threshold (delta) in seconds for when to delete duplicate events
# This function keeps the first (earliest) instance of an event when two occur close together
def deleteDuplicateEventRowsDelta(dataset,delta=120,event_name=None): 
    dataset = ArrangeRows(dataset,['case_id','new_time','event'])
    grouped = dataset.groupby('case_id')
    res = pd.DataFrame([])
    for case_id, group in grouped:
        group['match1'] = group.event.eq(group.event.shift()) 
        group['match2'] = group.event.eq(group.event.shift(-1)) 
        group['isRepeated'] = group.apply(lambda x: x['match1'] or x['match2'], axis = 1)
        group['delta'] = group['new_time'].diff()
        if event_name is None:
            group = group.drop(group[(group.isRepeated == True) & (group.match1 == True) & (group.delta < delta)].index)
        else:
            group = group.drop(group[(group.isRepeated == True) & (group.match1 == True) & (group.delta < delta) & (group.event == event_name)].index)
        res = pd.concat([res, group])
    res = res.reset_index(drop=True)
    return res

# Added by JT
# To anonymize event names a dataset, you can feed a map of
# search and replace values
def renameEventNames(dataset,replace_values):
    return dataset.replace({'event': replace_values})
    
# Added by JT
# To anonymize case IDs in a dataset to remove the risk of
# identifying someone based on the original case id
# This idea was inspired by Disco PM software's method
def anonymizeCaseIDs(dataset):
    dataset = ArrangeRows(dataset,['case_id','new_time'])
    grouped = dataset.groupby('case_id')
    res = pd.DataFrame([])
    c = 1
    for case_id, group in grouped:
        group = group.replace({'case_id': { case_id : c }})
        res = pd.concat([res,group])
        c += 1
    return res

# Added by JT
def getEventLogStartEvents(dataset):
    dataset = ArrangeRows(dataset,['case_id','timestamp'])
    start_events = []
    case_id = 0

    for index,row in dataset.iterrows():
        # check if we're at the start of a new case
        # if the case_id doesn't match the previous row, we have a new start event
        if case_id != row['case_id']:
             if row['event'] not in start_events:
                start_events.append(row['event'])
        case_id = row['case_id']
    start_events.sort(key=str.lower)
    return start_events

def getEventLogEndEvents(dataset):
    dataset = ArrangeRows(dataset,['case_id','timestamp'],False)
    end_events = []
    case_id = 0

    for index,row in dataset.iterrows():
        # check if we're at the start of a new case
        # if the case_id doesn't match the previous row, we have a new start event
        if case_id != row['case_id']:
             if row['event'] not in end_events:
                end_events.append(row['event'])
        case_id = row['case_id']
    end_events.sort(key=str.lower)
    return end_events

def getEventLogStats(dataset):
    total_cases = dataset['case_id'].nunique()
    total_events = dataset.shape[0]
    start_events = len(getEventLogStartEvents(dataset))
    end_events = len(getEventLogEndEvents(dataset))
    total_event_classes=dataset['event'].nunique()
    
    data = [{'Cases':total_cases,'Events':total_events,"Event Classes":total_event_classes,"Start events":start_events,"End events":end_events}]
    res = pd.DataFrame(data)
    return res


def getTraceDurations(dataset):
    dataset = ArrangeRows(dataset,['case_id','timestamp'])
    case_id = 0
    traces = []
    start_time = ""

    for index,row in dataset.iterrows():
        if case_id != row['case_id'] and case_id !=0:
            # we have a new start timestamp
            traces.append({
                'case_id' : case_id,
                'start' : start_time,
                'end'  : end_time
            })
            start_time = row['timestamp']
        
        if case_id == 0:
            start_time = row['timestamp']

        case_id = row['case_id']
        end_time = row['timestamp']

    res = pd.DataFrame(traces)
    res['start'] = pd.to_datetime(res['start'])
    res['end'] = pd.to_datetime(res['end'])
    res['time_delta'] = (res.end - res.start)
    res = ArrangeRows(res,['time_delta'],False)     
    return res


def filterTracesWithinDateRange(dataset,start_date,end_date,format="%d/%m/%Y %H:%M"):
    dataset = ArrangeRows(dataset,['case_id','timestamp'])
    case_id = 0
    start_date = dt.strptime(start_date,format)
    end_date = dt.strptime(end_date,format)

    for index,row in dataset.iterrows():
        # check if we're at the start of a new case
        # if the case_id doesn't match the previous row, we have a new start event
        if case_id != row['case_id'] and case_id !=0:
            if start_time < start_date or end_time > end_date:
                dataset = dataset[dataset.case_id != case_id]

            if type(row['timestamp']) == str:
                start_time = dt.strptime(row['timestamp'],format)
            else:
                start_time = row['timestamp']
            
        if case_id == 0: 
            if type(row['timestamp']) == str:
                start_time = dt.strptime(row['timestamp'],format)
            else:
                start_time = row['timestamp']

        case_id = row['case_id']

        if type(row['timestamp']) == str:
            end_time = dt.strptime(row['timestamp'],format)
        else:
            end_time = row['timestamp']

    return dataset