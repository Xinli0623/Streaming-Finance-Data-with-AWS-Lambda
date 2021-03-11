import json
import boto3
from datetime import datetime  
from datetime import timedelta  
import yfinance as yf 

kinesis = boto3.client('kinesis', "us-east-2")

def lambda_handler(event, context):
    print(datetime.now())
    caltime = datetime.now() + timedelta(hours=6) - timedelta(minutes=10)
    print(caltime)
    time = caltime.strftime("%H:%M")
    dt = "2020-12-01 "+time+":00-05:00"
    print(dt)
    
    tickers="FB,SHOP,BYND,NFLX,PINS,SQ,TTD,OKTA,SNAP,DDOG"
    data = yf.download(tickers,start="2020-12-01", end="2020-12-02", interval="1m")
    data=data.stack()
    data.reset_index(inplace=True)
    data["Datetime"]=data["Datetime"].astype("str")
    data=data.rename(columns={"High": "high", "Low": "low","Datetime":"ts","level_1":"name"})
    data=data[["high","low","ts","name"]]
    data=data[data["ts"]==dt]
    data=data.to_json(orient="records",lines=True)
    
    kinesis.put_record(
            StreamName="STA9760F2020_project03",
            Data=data,
            PartitionKey="partitionkey")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
        }