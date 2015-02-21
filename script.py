#!/usr/bin/python
import time
import sys
import datetime
from datetime import timedelta
from dateutil import parser
import gzip
import subprocess ## for making system calls
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
import os

reload(sys);
sys.setdefaultencoding("utf8")

######function definitions start

## compress our large CSV file. Google cloud store requires that you upload a gzipped version for large files
## more on creating the CSV file can be found here: https://cloud.google.com/bigquery/preparing-data-for-bigquery 
def gzipDataFile():
	f_in = open(csvFilePath, 'rb')
	f_out = gzip.open(csvFilePath+'.gz', 'wb')
	f_out.writelines(f_in)
	f_out.close()
	f_in.close()


## call gsutil to upload our gzipped file in Google Cloud Storage.
## the file is stored in appropriate bucket you mention at the start of script
## more on gsutil can be found here: https://cloud.google.com/storage/docs/gsutil
def uploadToGC():
	now = datetime.datetime.now()	
	return_code = subprocess.call(executablePath+"/gsutil cp "+csvFilePath+".gz gs://"+bucket+"/"+`now.year`+"/"+`now.month`+"/"+`now.day`+"/"+moduleName+".csv.gz", shell=True)  


## create the data set if not already present.
## if present the utility does not break

def createDataset():
	return_code = subprocess.call(executablePath+"/bq mk "+dataset, shell=True)
	

## we create a temporary table with all fields having type as string to start with.
## later we use data from this table with appropriate type associate to put in actual table
def createTable():
	
	return_code = subprocess.call(executablePath+"/bq mk -t "+dataset+"."+tempTable+" "+schema, shell=True)
	

## populate the temp table by reading the gzipped file in Google cloud store
## Notice the command line options mentioned.
## more on creating table options: https://cloud.google.com/bigquery/bq-command-line-tool#creatingtablefromfile
def loadDataInTable():
	now = datetime.datetime.now()
	return_code = subprocess.call(executablePath+"/bq load --field_delimiter=',' --source_format=CSV --skip_leading_rows=1 --max_bad_records=10 --format=csv --encoding=UTF-8 "+dataset+"."+tempTable+" gs://"+bucket+"/"+`now.year`+"/"+`now.month`+"/"+`now.day`+"/"+moduleName+".csv.gz "+schema,shell=True)
	copyTable()
	

## now the actual part
## copy the data from temp table to actual table with sanity
def copyTable():
	sql = sqlString+" ["+dataset+"."+tempTable+"]"
	return_code = subprocess.call(executablePath+"/bq query --allow_large_results=true --append_table=true --destination_table="+dataset+"."+tableMaster+" \""+sql+"\"",shell=True)


## remove the files 
def removeFromGC():
	now = datetime.datetime.now()
	return_code = subprocess.call(executablePath+"/gsutil rm gs://"+bucket+"/"+`now.year`+"/"+`now.month`+"/"+`now.day`+"/"+moduleName+".csv.gz", shell=True)

## Delete the temp tables we created. It is no more needed
def deleteTempTable():
	return_code = subprocess.call(executablePath+"/bq rm -f "+dataset+"."+tempTable,shell=True)


## finally send some stats about your table
## here I am sending a daywise count of the data 
def sendCountEmail():
	countFile = "/tmp/"+moduleName+"count.txt"
        sql = "SELECT day(created_time) as dayOfMonth,count(*) as Count FROM ["+dataset+"."+tableMaster+"] group by dayOfMonth order by dayOfMonth asc"  
        return_code = subprocess.call(executablePath+"/bq query \""+sql+"\" > "+countFile,shell=True)
	logging.info(executablePath+"/bq query \""+sql+"\" > "+countFile)
	msg = MIMEMultipart('alternative')
	recipients = ['some1@some1.com','some2@some2.com']
	smtpObj = smtplib.SMTP('xxx.xxx.xxx.xxx')
	msg['Subject'] = 'Count of '+moduleName+' uploaded in BigQuery'
	msg['To'] = ", ".join(recipients)
	msg['From'] = 'from@from.com'
	body = 'PFA the count of '+moduleName+' \n'
	os.chmod(countFile, 0777)
	with open(countFile,'r') as handle:
		for line in handle:
			body += line+"\n"

	f = file(countFile)
	attachment = MIMEText(f.read())
	attachment.add_header('Content-Disposition', 'attachment', filename='count.txt') 
          
	msg.attach(attachment)
	msg.attach(MIMEText(body))

	smtpObj.sendmail('service@quikr.com', recipients, msg.as_string())


######function definitions end


## ALL starts from here
moduleName="ANYTHING_YOU_WISH"
## create schema to temporary table
schema="id:STRING,imei:STRING,app_version:STRING,created_time:STRING,referral:STRING,install_type:STRING,ip_addr:STRING,device_uid:STRING,email:STRING,model:STRING,source_db:STRING,line1:STRING,os_version:STRING,location:STRING,source:STRING,campaign:STRING"

## we will use the below SQL to copy data from temp table to actual table
sqlString="SELECT INTEGER(id) as id,FLOAT(app_version) as app_version,imei,FORMAT_UTC_USEC((INTEGER(created_time)+19800)*1000000) as created_time,referral,INTEGE (install_type) as install_type,ip_addr,device_uid,email,model,source_db,line1,os_version,location,source,campaign FROM"


## day when script will run
now = datetime.datetime.now()

## path to your installed google cloud SDK on server
executablePath = "/path/to/google-cloud-sdk/bin"

## bucket name to store the CSV file (zipped version) on Google Cloud Storage
bucket = "YOUR_BUCKET_NAME"

## the data set name you wish to create your tables in
dataset = "YOUR_DATASET_NAME"

## this will be your CSV file path
csvFilePath = "/tmp/"+moduleName+"_.csv"

## our temp table name
tempTable = moduleName+"_temp"

## our actual table name
tableMaster= moduleName+"_master"


##### create gz file for upload
gzipDataFile()


#### uploading appropriate bucket
uploadToGC()


###  create datasets on bigquery
createDataset()

###  create table on bigquery
createTable()

###  load data table on bigquery
loadDataInTable()


###  remove uploaded data from GC
removeFromGC()

## send stats
sendCountEmail()

## delete the temp table
deleteTempTable()
