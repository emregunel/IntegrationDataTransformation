from __future__ import print_function
import json
import boto3
import pandas as pd
import numpy as np
from datetime import date, timedelta

def lambda_handler(event, context):
    #old Csv Links
    brandOldLink='https://s3.eu-central-1.amazonaws.com/salesforce-delivery/livedataBrand.csv'
    performanceOldLink='https://s3.eu-central-1.amazonaws.com/salesforce-delivery/livedataPerformance.csv'


    #get old data as pandas dataframe
    brandData=pd.read_csv(brandOldLink, delimiter=";")
    performanceData=pd.read_csv(performanceOldLink,delimiter=";")

	##added new files for logging
	brand_log_string=brandData.to_csv(header=True,index=False)
	performance_log_string=performanceData.to_csv(header=True,index=False)


    #convert date object to pd datetime
    brandData['date'] =  pd.to_datetime(brandData['date'], format='%Y-%m-%d')
    performanceData['date'] =  pd.to_datetime(performanceData['date'], format='%Y-%m-%d')

    #add month column
    brandData['month']=brandData['date'].apply(lambda x: str(x)[0:7])
    performanceData['month']=performanceData['date'].apply(lambda x: str(x)[0:7])

    #Roll-up pivot table for brand
    brandPivotTable = pd.pivot_table(brandData, values=['impressions','cvvs','clicks'],\
                         index=['placement_id','month'],  aggfunc=np.sum)

    #Roll-up pivot table for performance
    performancePivotTable = pd.pivot_table(performanceData, values=['spend','installs','clicks','impressions'],\
                         index=['placement_id','month'],  aggfunc=np.sum)

    #get new dataFrames
    brandFlattened = pd.DataFrame(brandPivotTable.to_records())
    performanceFlattened = pd.DataFrame(performancePivotTable.to_records())

    #convert df's to string
    brandX = brandFlattened.to_string(header=True,
                      index=False,
                      index_names=False).split('\n')
    brand_encoded_string = brandFlattened.to_csv(header=True,index=False)
    performance_encoded_string = performanceFlattened.to_csv(header=True,index=False)
    

    bucket_name = "salesforce-wise"
    file_name="brand.csv"
    lambda_path = "/tmp/" + file_name
    s3_path = "/csv/" + file_name

    s3 = boto3.resource("s3")
    s3.Bucket(bucket_name).put_object(Key=s3_path, Body=brand_encoded_string, ACL='public-read')

    file_name="performance.csv"
    lambda_path = "/tmp/" + file_name
    s3_path = "/csv/" + file_name

    s3 = boto3.resource("s3")
    s3.Bucket(bucket_name).put_object(Key=s3_path, Body=performance_encoded_string, ACL='public-read')


    file_name=str(date.today()) + "-Brand_Log.csv"
    lambda_path = "/tmp/" + file_name
    s3_path = "/csv/" + file_name


    s3 = boto3.resource("s3")
    s3.Bucket(bucket_name).put_object(Key=s3_path, Body=brand_log_string, ACL='public-read')

	file_name = str(date.today()) + "-Performance_Log.csv"
	lambda_path = "/tmp/" + file_name
    s3_path = "/csv/" + file_name

    s3 = boto3.resource("s3")
    s3.Bucket(bucket_name).put_object(Key=s3_path, Body=performance_log_string, ACL='public-read')
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
