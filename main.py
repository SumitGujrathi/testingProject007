from datetime import datetime, time
import time
import requests
import pandas as pd
import numpy as np
import pymongo


# List of Holidays Important *** Date formate should be like MM-DD-YYYY eg. 01-01-2023****

OffDates = ["01-26-2023","03-07-2023","03-30-2023","04-04-2023","04-07-2023","04-14-2023","05-01-2023",
"06-28-2023","08-15-2023","09-19-2023","10-02-2023","10-24-2023","11-14-2023","11-27-2023","12-25-2023",
"01-01-2023","01-07-2023","01-08-2023","01-14-2023","01-15-2023","01-21-2023","01-22-2023","01-28-2023",
"01-29-2023","02-12-2023","02-18-2023","02-19-2023","02-25-2023",
"02-26-2023","03-04-2023","03-05-2023","03-11-2023","03-12-2023","03-18-2023","03-19-2023","03-25-2023",
"03-26-2023","04-01-2023","04-02-2023","04-08-2023","04-09-2023","04-15-2023","04-16-2023","04-22-2023",
"04-23-2023","04-29-2023","04-30-2023","05-06-2023","05-07-2023","05-13-2023","05-14-2023","05-20-2023",
"05-21-2023","05-27-2023","05-28-2023","06-03-2023","06-04-2023","06-10-2023","06-11-2023","06-17-2023",
"06-18-2023","06-24-2023","06-25-2023","07-01-2023","07-02-2023","07-08-2023","07-09-2023","07-15-2023",
"07-16-2023","07-22-2023","07-23-2023","07-29-2023","07-30-2023","08-05-2023","08-06-2023","08-12-2023",
"08-13-2023","08-19-2023","08-20-2023","08-26-2023","08-27-2023","09-02-2023","09-03-2023","09-09-2023",
"09-10-2023","09-16-2023","09-17-2023","09-23-2023","09-24-2023","09-30-2023","10-01-2023","10-07-2023",
"10-08-2023","10-14-2023","10-15-2023","10-21-2023","10-22-2023","10-28-2023","10-29-2023","11-04-2023",
"11-05-2023","11-11-2023","11-12-2023","11-18-2023","11-19-2023","11-25-2023","11-26-2023","12-02-2023",
"12-03-2023","12-09-2023","12-10-2023","12-16-2023","12-17-2023","12-23-2023","12-24-2023","12-30-2023","12-31-2023"
]

# NSE Data Pull + Data Clearing + MongoDB Funciton

def Pull_Chain_Data(Symbol):

    # MongoDB Connection

    Dataset_Name = "NSE"
    Collection = Symbol

    linkMD = f"mongodb+srv://sumitgujrathi24:8lpVTDIBtyHKWRH3@sumit.ybjewjm.mongodb.net/{Dataset_Name}?retryWrites=true&w=majority"

    Client = pymongo.MongoClient(linkMD, 27017)

    db = Client[Dataset_Name]

    DATA_NSE = db[Collection]

      
    # Date  format => YYYY,MM,DD, HH,MM,SS
    Pull_Time = datetime.now()
    OI = {"Date":Pull_Time, "PE_OI": 1,"CE_OI":1, "PE_COI":1, "CE_COI":1, "NIFTY":1, "PCR":1, "PE_Volatility": 0, "CE_Volatility": 1}
    DATA_NSE.insert_one(OI)
    print(OI(

 
Pull_Chain_Data(Symbol="NIFTY")

# Date Setting 

Today_Date = datetime.now().strftime('%m-%d-%Y')
print(Today_Date)


# On_Day = Today_Date in OffDates # If False Market is On.
On_Day = Today_Date in OffDates

# print(On_Day)


# Start Time Setting 

cur_time = datetime.now().strftime('%H:%M:%S')
cur_time = datetime.strptime(cur_time, '%H:%M:%S').time()
print(cur_time)


