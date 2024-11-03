import json
import boto3
from  io import StringIO 
import pandas as pd

s3_client=boto3.client('s3')
sns_client=boto3.client('sns')
sns_arn='arn:aws:sns:ap-south-1:195275642738:s3-arrival-notification'

def lambda_handler(event, context):
 try:
    print(event)
    bucket=event['Records'][0]['s3']['bucket']['name']
    key=event['Records'][0]['s3']['object']['key']
    
    # print('bucket name',bucket)
    print('file name',key)
    response=s3_client.get_object(Bucket=bucket,Key=key)
    # print(response['Body'])
    file_data=response['Body'].read().decode('utf-8')
    df=pd.read_csv(StringIO(file_data))
    print(df.head(10))

    message=f's3 file {key} has been processed successfully'
    resp2=sns_client.publish(Subject='SUCCSESS-Daily data processed',TargetArn=sns_arn,Message=message,MessageStructure='text')
 except Exception as e:
    print(e)
    message='input s3 file processing failed'
    resp2=sns_client.publish(Subject='unable to process-Daily data',TargetArn=sns_arn,Message=message,MessageStructure='text')