import json
import time
import consonants as constant
import pytz
import datetime
from datetime import timedelta
from pytz import timezone
from pandas import json_normalize
import requests
# import pyarrow
import boto3
from dateutil.tz import tzutc, UTC
import pandas as pd
from pandas import DataFrame
import numpy as np
from io import BytesIO
from io import StringIO
from json import dumps
from pandas.io.json import json_normalize
# from datetime import datetime as dt
bucket = 'data-ras'  # already created on S3
client = boto3.client('ssm')
sns = boto3.client("sns", region_name="ap-northeast-1")
ssm = boto3.client("ssm", region_name="ap-northeast-1")
s3_resource = boto3.resource('s3')
url = ssm.get_parameter(Name=constant.urlapi, WithDecryption=True)["Parameter"]["Value"]
# url = "https://results.us.securityeducation.com/api/reporting/v0.1.0/phishing"
my_headers = {'x-apikey-token': 'YV5iYWWji13z25LX/ZAQOwmHKmHqdpQk8pQzIcSQ5hGl3cpog'}
s3_prefix = "result/csvfiles"


def get_datetime():
    dt = datetime.datetime.now()
    return dt.strftime("%Y%m%d"), dt.strftime("%H:%M:%S")


datestr, timestr = get_datetime()
fname = f"data_api_tripler_{datestr}_{timestr}.csv"
file_prefix = "/".join([s3_prefix, fname])
file = [line.strip() for line in url]


def send_sns_success():
    success_sns_arn = ssm.get_parameter(Name=constant.SUCCESSNOTIFICATIONARN, WithDecryption=True)["Parameter"]["Value"]
    component_name = constant.COMPONENT_NAME
    env = ssm.get_parameter(Name=constant.ENVIRONMENT, WithDecryption=True)['Parameter']['Value']
    success_msg = constant.SUCCESS_MSG
    sns_message = (f"{component_name} :  {success_msg}")
    print(sns_message, 'text')
    succ_response = sns.publish(TargetArn=success_sns_arn, Message=json.dumps({'default': json.dumps(sns_message)}),
                                Subject=env + " : " + component_name, MessageStructure="json")
    return succ_response


def send_error_sns():
    error_sns_arn = ssm.get_parameter(Name=constant.ERRORNOTIFICATIONARN)["Parameter"]["Value"]
    env = ssm.get_parameter(Name=constant.ENVIRONMENT, WithDecryption=True)['Parameter']['Value']
    error_message = constant.ERROR_MSG
    component_name = constant.COMPONENT_NAME
    sns_message = (f"{component_name} : {error_message}")
    err_response = sns.publish(TargetArn=error_sns_arn, Message=json.dumps({'default': json.dumps(sns_message)}),
                               Subject=env + " : " + component_name,
                               MessageStructure="json")
    return err_response


def reshape(r):
    dt = r["data"]
    at = dt[0]["attributes"]
    res = {
        "Type": dt[0]['type'],
        "id": dt[0]["id"],
        "UserFirstname": at['userfirstname'],
        "userLastname": at['userlastname'],
        "useremailaddress": at['useremailaddress'],
        "useractiveflag": at['useractiveflag'],
        "userdeleteddate": at['userdeleteddate'],
        "senttimestamp": at['senttimestamp'],
        "eventtimestamp": at['eventtimestamp'],
        "eventtype": at['eventtype'],
        "campaignname": at['campaignname'],
        "autoenrollment": at['autoenrollment'],
        "campaignstartdate": at['campaignstartdate'],
        "campaignenddate": at['campaignenddate'],
        "campaigntype": at['campaigntype'],
        "campaignstatus": at['campaignstatus'],
        "templatename": at['templatename'],
        "templatesubject": at['templatesubject'],
        "assessmentisarchived": at['assessmentisarchived'],
        "usertags": at['usertags'],
        "sso_id": at['sso_id']
    }
    return res


r = requests.get(url, headers=my_headers)
responses = []
responses.append(reshape(r.json()))
serialized = []
for r in responses:
    serialized.append(json.dumps(r))
    jsonlines_doc = "\n".join(serialized)


def lambda_handler(event, context):
    try:
        df = pd.read_json(jsonlines_doc, lines=True)
        csv_buffer1 = StringIO()
        df.to_csv(csv_buffer1)
        # Writing the Files to CSV
        s3_resource.Object(bucket, file_prefix).put(Body=csv_buffer1.getvalue())
        print('CSV files written')
        ####SUCCESS-SNS-NOTIFICATION ACTIVATED#####
        send_sns_success()
        print('Email delivered')
    except Exception as e:
        error_message = str(e)
        print(error_message)

    # send_error_sns("SYSTEM_ERRORS", "TECHNICAL_ERROR", "LAMBDA_ERRORS", error_message=str(e))
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hurry!!! ..Dataframe Created and loaded !')
    }
