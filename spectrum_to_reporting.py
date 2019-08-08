
import boto3
import sqlalchemy
import os
from sqlalchemy import text
import simplejson
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
import pandas as pd
import datetime
import subprocess

#Open Config File That contains credentials
#
try:
    with open("./redshift_creds.json.nogit") as fh:
        creds = simplejson.loads(fh.read())
except Exception as e:
    print(e)

#Create Sql Alchemy Connection To Flywheel Redshift

connect_to_fw_db = create_engine('postgresql://' + creds['user_name'] + ':' + creds['password'] + '@' + creds['host_name_fw_prod'] + ':' + creds['port_num'] + '/' + creds['db_name_fw_prod']);


#Run Flywheel Side of Script

fw_sql = '''

--Author: George Sarfo
--Desc: PnG All in One SQL Script to load and unload various sources (Kroger, Instacart, Shipt etc) from Data Engineering Team to Data Science Team
--Date: 07/25/2019
--Ticket(s): PS-5755, PS-5756


-----------Create External Tables----------------------------

--NB: Drop and Create May not be needed and can be commented out as this uses SPECTRUM and data is already present in external table
--Left External Table Creation Here for Readability and Understanding of Process
--Instead of deleting, Comment it out when you wish to remove it



-----------Insertion From Spectrum Staging into Reporting Tables Begins Below---------------------------------------------------------

--truncate table fw.fact_pg_datascience_instacart_sov_daily;

insert into fw.fact_pg_datascience_instacart_sov_daily

(
Country ,
Retailer ,
UTCTime ,
Keyword ,
Zip ,
Store ,
Placement ,
Brand ,
SKU,
Page,
Search_Dept_Sponsored ,
Search_Dept ,
Rank,
Platform ,
Title ,
Price,
Unit ,
Size ,
URL ,
Coupon ,
Zone_Name ,
Zone_ID ,
file_name
)

select 
distinct
Country ,
Retailer ,
UTCTime ,
Keyword ,
Zip ,
Store ,
Placement ,
Brand ,
SKU,
Page,
Search_Dept_Sponsored ,
Search_Dept ,
Rank,
Platform ,
Title ,
Price,
Unit ,
Size ,
URL ,
Coupon ,
Zone_Name ,
Zone_ID  ,
"$path" as file_name
from  spectrum.instacart_ds_sov_staging ;
--where TRUNC(utctime::date) = getdate()-1 ;


------------START SHIPT-DS PROCESS LOAD TO REPORTING/FACT TABLE-------------------------

-----------------Insert to Reporting Table starts below----------------------------------

--truncate table fw.fact_pg_datascience_shipt_sov_daily;

insert into fw.fact_pg_datascience_shipt_sov_daily
(
Country ,
Client_Name ,
Business_Unit ,
Retailer ,
UTCTime ,
Search_Zip ,
Zip ,
Keyword ,
Placement ,
Brand ,
SKU ,
Page ,
Rank ,
Price ,
List_Price ,
URL ,
Title ,
Average_Rating ,
Total_Reviews ,
Image_URL ,
Tags,
File_Name
)
select 
distinct
Country ,
Client_Name ,
Business_Unit ,
Retailer ,
UTCTime ,
Search_Zip ,
Zip ,
Keyword ,
Placement ,
Brand ,
SKU ,
Page ,
Rank ,
Price ,
List_Price ,
URL ,
Title ,
Average_Rating ,
Total_Reviews ,
Image_URL ,
Tags,
"$path" as File_Name
from spectrum.shipt_ds_sov_staging
--where TRUNC(utctime) = getdate()-1
;

------------Target-----------------

--truncate table fw.fact_pg_datascience_target_sov_daily;

INSERT into fw.fact_pg_datascience_target_sov_daily
(
country,
       client_name,
       business_unit,
       retailer,
       utctime,
       search_zip,
       zip,
       keyword,
       placement,
       brand,
       sku,
       page,
       rank,
       price,
       list_price,
       url,
       title,
       average_rating,
       total_reviews,
       image_url,
       file_name
)
SELECT distinct country,
       client_name,
       business_unit,
       retailer,
       utctime,
       search_zip,
       zip,
       keyword,
       placement,
       brand,
       sku,
       page,
       rank,
       price,
       list_price,
       url,
       title,
       average_rating,
       total_reviews,
       image_url,
       "$path" as file_name
       
FROM spectrum.target_ds_sov_staging;


------------Walmart CA-----------------

--truncate table fw.fact_pg_datascience_walmart_ca_sov_daily;

insert into fw.fact_pg_datascience_walmart_ca_sov_daily

(
country,
       client_name,
       business_unit,
       retailer,
       utctime,
       keyword,
       placement,
       brand,
       sku,
       page,
       absolute_rank,
       rank,
       price,
       url,
       title,
       average_rating,
       total_reviews,
       image_url,
       tags,
       file_name
)
SELECT distinct country,
       client_name,
       business_unit,
       retailer,
       utctime,
       keyword,
       placement,
       brand,
       sku,
       page,
       absolute_rank,
       rank,
       price,
       url,
       title,
       average_rating,
       total_reviews,
       image_url,
       tags,
       "$path" as file_name
FROM spectrum.walmart_ds_ca_sov_staging ;




-----------Walmart US---------------------

--truncate table fw.fact_pg_datascience_walmart_sov_daily;

insert into fw.fact_pg_datascience_walmart_sov_daily

(
country,
       client_name,
       business_unit,
       retailer,
       utctime,
       keyword,
       placement,
       brand,
       sku,
       page,
       absolute_rank,
       rank,
       price,
       url,
       title,
       average_rating,
       total_reviews,
       image_url,
       tags,
       file_name
)
SELECT distinct country,
       client_name,
       business_unit,
       retailer,
       utctime,
       keyword,
       placement,
       brand,
       sku,
       page,
       absolute_rank,
       rank,
       price,
       url,
       title,
       average_rating,
       total_reviews,
       image_url,
       tags,
       "$path" as file_name
FROM spectrum.walmart_ds_sov_staging ;

'''

connect_to_fw_db.execute(text(fw_sql))

#sql statements to get file_names from spectrum staging tables

get_file_names_shipt='''select distinct "$path" from  spectrum.shipt_ds_sov_staging '''

get_file_names_instacart='''select distinct "$path" from  spectrum.instacart_ds_sov_staging'''

get_file_names_target='''select distinct "$path" from  spectrum.target_ds_sov_staging'''

get_file_names_walmart='''select distinct "$path" from  spectrum.walmart_ds_sov_staging'''

get_file_names_walmart_ca='''select distinct "$path" from  spectrum.walmart_ds_ca_sov_staging'''



#query prepared statements to get file names from spectrum staging tables

result_shipt = connect_to_fw_db.execute(text(get_file_names_shipt))

result_instacart = connect_to_fw_db.execute(text(get_file_names_instacart))

result_target= connect_to_fw_db.execute(text(get_file_names_target))

result_walmart= connect_to_fw_db.execute(text(get_file_names_walmart))

result_walmart_ca= connect_to_fw_db.execute(text(get_file_names_walmart_ca))


#get result set of file_names into lists

result_instacart_list=list(result_instacart)
result_shipt_list=list(result_shipt)
result_target_list=list(result_target)
result_walmart_list=list(result_walmart)
result_walmart_ca_list=list(result_walmart_ca)



#clear existing bucket
'''
print("Clearing Destination Buckets")

s3 = boto3.client('s3')
resp = s3.list_objects_v2(Bucket='fw-pg-datascience', Prefix='Shipt SOV')

clear_cmd = 'aws s3 rm \"s3://fw-pg-datascience/Shipt SOV/\" --recursive'

p = subprocess.Popen(clear_cmd, shell=True, stdout=subprocess.PIPE)
p.communicate()


clear_cmd = 'aws s3 rm \"s3://fw-pg-datascience/Instacart SOV/\" --recursive'

p = subprocess.Popen(clear_cmd, shell=True, stdout=subprocess.PIPE)
p.communicate()

clear_cmd = 'aws s3 rm \"s3://fw-pg-datascience/Target SOV/\" --recursive'

p = subprocess.Popen(clear_cmd, shell=True, stdout=subprocess.PIPE)
p.communicate()


clear_cmd = 'aws s3 rm \"s3://fw-pg-datascience/Walmart SOV/\" --recursive'

p = subprocess.Popen(clear_cmd, shell=True, stdout=subprocess.PIPE)
p.communicate()


clear_cmd = 'aws s3 rm \"s3://fw-pg-datascience/Walmart CA SOV/\" --recursive'

p = subprocess.Popen(clear_cmd, shell=True, stdout=subprocess.PIPE)
p.communicate()
'''


#Uload Contents of Reporting Tables
print("Unloading Contents of Various Tables")

for r in result_shipt_list:

    print("unloading contents from "+str(r))

    unload_shipt = '''
    -----------Unload SHIPT data from Reporting table to datascience S3-------------------------------
    unload (\'select * from fw.fact_pg_datascience_shipt_sov_daily where File_Name = {0} \') 
    to \'s3://fw-pg-datascience/Shipt SOV/{1}\' 
    credentials \'aws_iam_role=arn:aws:iam::641992447897:role/RedshiftAccessToS3\' header delimiter ',' addquotes allowoverwrite parallel off;    
    '''.format(str(r).replace('(', '').replace(')', '').replace('\'', '\'\'').replace(',',''), str(r).replace('(', '').replace(')', '').replace('\'', '').replace(',','').replace('s3://fw-sov/Instacart-DS/', '').replace('s3://fw-sov/Shipt-DS/', ''))

    connect_to_fw_db.execute(text(unload_shipt))

    # Archive File after loading to reporting table

    archive_cmd = 'aws s3 cp {0} s3://fw-sov/Shipt-DSArchive/'.format(str(r).replace('(', '').replace(')', '').replace('\'', '').replace(',', ''))

    print('archiving file with ' + archive_cmd)

    p = subprocess.Popen(archive_cmd, shell=True, stdout=subprocess.PIPE)
    p.communicate()

    # remove file
    archive_cmd = 'aws s3 rm {0}'.format(str(r).replace('(', '').replace(')', '').replace('\'', '').replace(',', ''))

    p = subprocess.Popen(archive_cmd, shell=True, stdout=subprocess.PIPE)
    p.communicate()

    print("shipt unload completed")

for r in result_instacart_list:

    print("unloading contents from "+str(r))

    unload_instacart = '''unload ('select * from fw.fact_pg_datascience_instacart_sov_daily where file_name = {0} ') 
    to \'s3://fw-pg-datascience/Instacart SOV/{1}\' 
    credentials 'aws_iam_role=arn:aws:iam::641992447897:role/RedshiftAccessToS3' header delimiter ',' addquotes allowoverwrite parallel off;
    
    '''.format(str(r).replace('(', '').replace(')', '').replace('\'', '\'\'').replace(',',''), str(r).replace('(', '').replace(')', '').replace('\'', '').replace(',','').replace('s3://fw-sov/Instacart-DS/', '').replace('s3://fw-sov/Shipt-DS/', ''))

    connect_to_fw_db.execute(text(unload_instacart))

    # Archive File after loading to reporting table

    archive_cmd = 'aws s3 cp {0} s3://fw-sov/Instacart-DSArchive/'.format(str(r).replace('(', '').replace(')', '').replace('\'', '').replace(',', ''))

    print('archiving file with ' + archive_cmd)

    p = subprocess.Popen(archive_cmd, shell=True, stdout=subprocess.PIPE)
    p.communicate()

    # remove file
    archive_cmd = 'aws s3 rm {0}'.format(str(r).replace('(', '').replace(')', '').replace('\'', '').replace(',', ''))

    p = subprocess.Popen(archive_cmd, shell=True, stdout=subprocess.PIPE)
    p.communicate()

    print('Instacart Unload Completed')

for r in result_target_list:
    print("unloading contents from " + str(r))

    unload_target = '''unload ('select * from fw.fact_pg_datascience_target_sov_daily where file_name = {0} ') 
    to \'s3://fw-pg-datascience/Target SOV/{1}\' 
    credentials 'aws_iam_role=arn:aws:iam::641992447897:role/RedshiftAccessToS3' header delimiter ',' addquotes allowoverwrite parallel off;

    '''.format(str(r).replace('(', '').replace(')', '').replace('\'', '\'\'').replace(',', ''),
               str(r).replace('(', '').replace(')', '').replace('\'', '').replace(',', '').replace(
                   's3://fw-sov/Target-DS/', '').replace('s3://fw-sov/Target-DS/', ''))

    connect_to_fw_db.execute(text(unload_target))


    #Archive File after loading to reporting table

    archive_cmd = 'aws s3 cp {0} s3://fw-sov/Target-DSArchive/'.format(str(r).replace('(', '').replace(')', '').replace('\'', '').replace(',', ''))

    print('archiving file with ' + archive_cmd)

    p = subprocess.Popen(archive_cmd, shell=True, stdout=subprocess.PIPE)
    p.communicate()

    #remove file
    archive_cmd = 'aws s3 rm {0}'.format(str(r).replace('(', '').replace(')', '').replace('\'', '').replace(',', ''))

    p = subprocess.Popen(archive_cmd, shell=True, stdout=subprocess.PIPE)
    p.communicate()


    print("Target completed")



for r in result_walmart_list:
    print("unloading contents from " + str(r))

    unload_target = '''unload ('select * from fw.fact_pg_datascience_walmart_sov_daily where file_name = {0} ') 
    to \'s3://fw-pg-datascience/Walmart SOV/{1}\' 
    credentials 'aws_iam_role=arn:aws:iam::641992447897:role/RedshiftAccessToS3' header delimiter ',' addquotes allowoverwrite parallel off;

    '''.format(str(r).replace('(', '').replace(')', '').replace('\'', '\'\'').replace(',', ''),
               str(r).replace('(', '').replace(')', '').replace('\'', '').replace(',', '').replace(
                   's3://fw-sov/Walmart-DS/', '').replace('s3://fw-sov/Walmart-DS/', ''))

    connect_to_fw_db.execute(text(unload_target))

    #Archive File after loading to reporting table

    archive_cmd = 'aws s3 cp {0} s3://fw-sov/Walmart-DSArchive/'.format(str(r).replace('(', '').replace(')', '').replace('\'', '').replace(',', ''))

    print('archiving file with ' + archive_cmd)

    p = subprocess.Popen(archive_cmd, shell=True, stdout=subprocess.PIPE)
    p.communicate()

    #remove file
    archive_cmd = 'aws s3 rm {0}'.format(str(r).replace('(', '').replace(')', '').replace('\'', '').replace(',', ''))

    p = subprocess.Popen(archive_cmd, shell=True, stdout=subprocess.PIPE)
    p.communicate()

    print('Walmart US Unload Completed')



for r in result_walmart_ca_list:
    print("unloading contents from " + str(r))

    unload_target = '''unload ('select * from fw.fact_pg_datascience_walmart_ca_sov_daily where file_name = {0} ') 
    to \'s3://fw-pg-datascience/Walmart CA SOV/{1}\' 
    credentials 'aws_iam_role=arn:aws:iam::641992447897:role/RedshiftAccessToS3' header delimiter ',' addquotes allowoverwrite parallel off;

    '''.format(str(r).replace('(', '').replace(')', '').replace('\'', '\'\'').replace(',', ''),
               str(r).replace('(', '').replace(')', '').replace('\'', '').replace(',', '').replace(
                   's3://fw-sov/WalmartCA-DSNew/', '').replace('s3://fw-sov/WalmartCA-DS/', ''))

    connect_to_fw_db.execute(text(unload_target))

    #Archive File after loading to reporting table

    archive_cmd = 'aws s3 cp {0} s3://fw-sov/WalmartCA-DSNewArchive/'.format(str(r).replace('(', '').replace(')', '').replace('\'', '').replace(',', ''))

    print('archiving file with ' + archive_cmd)

    p = subprocess.Popen(archive_cmd, shell=True, stdout=subprocess.PIPE)
    p.communicate()

    #remove file
    archive_cmd = 'aws s3 rm {0}'.format(str(r).replace('(', '').replace(')', '').replace('\'', '').replace(',', ''))

    p = subprocess.Popen(archive_cmd, shell=True, stdout=subprocess.PIPE)
    p.communicate()

    print('Walmart CA Unload Completed')

print("unloads completed")

print('')


#Rename Files to remove 000 at end of .csv
print("Renaming Files")

s3 = boto3.client('s3')
resp = s3.list_objects_v2(Bucket='fw-pg-datascience', Prefix='Shipt SOV/')

print(resp)
for obj in resp['Contents']:

    if 'csv000' in obj:

        f_name=obj['Key']
        new_f_name=f_name.split('.', 1)[0] + '.csv'
        print(new_f_name)
        #print(obj)

        s3 = boto3.resource('s3')

        print("Copying " + f_name + " To "+new_f_name)
        rename_cmd = 'aws s3 cp \"s3://fw-pg-datascience/' + f_name + '\" \"s3://fw-pg-datascience/'+new_f_name+'\"'

        del_cmd = 'aws s3 rm \"s3://fw-pg-datascience/' + f_name + '\"'

        p = subprocess.Popen(rename_cmd, shell=True, stdout=subprocess.PIPE)
        p.communicate()

        print("Deleting Old " + f_name )

        pp=subprocess.Popen(del_cmd, shell=True, stdout=subprocess.PIPE)
        pp.communicate()


s3 = boto3.client('s3')
resp = s3.list_objects_v2(Bucket='fw-pg-datascience', Prefix='Instacart SOV/')

print(resp)
for obj in resp['Contents']:

    if 'csv000' in obj:

        f_name=obj['Key']
        new_f_name=f_name.split('.', 1)[0] + '.csv'
        print(new_f_name)
        #print(obj)

        s3 = boto3.resource('s3')

        print("Copying " + f_name + " To " + new_f_name)
        rename_cmd = 'aws s3 cp \"s3://fw-pg-datascience/' + f_name + '\" \"s3://fw-pg-datascience/' + new_f_name + '\"'

        del_cmd = 'aws s3 rm \"s3://fw-pg-datascience/' + f_name + '\"'

        p = subprocess.Popen(rename_cmd, shell=True, stdout=subprocess.PIPE)
        p.communicate()

        print("Deleting Old " + f_name  )
        pp = subprocess.Popen(del_cmd, shell=True, stdout=subprocess.PIPE)
        pp.communicate()

s3 = boto3.client('s3')
resp = s3.list_objects_v2(Bucket='fw-pg-datascience', Prefix='Target SOV/')

print(resp)
for obj in resp['Contents']:

    if 'csv000' in obj:

        f_name=obj['Key']
        new_f_name=f_name.split('.', 1)[0] + '.csv'
        print(new_f_name)
        #print(obj)

        s3 = boto3.resource('s3')

        print("Copying " + f_name + " To " + new_f_name)
        rename_cmd = 'aws s3 cp \"s3://fw-pg-datascience/' + f_name + '\" \"s3://fw-pg-datascience/' + new_f_name + '\"'

        del_cmd = 'aws s3 rm \"s3://fw-pg-datascience/' + f_name + '\"'

        p = subprocess.Popen(rename_cmd, shell=True, stdout=subprocess.PIPE)
        p.communicate()

        print("Deleting Old " + f_name  )
        pp = subprocess.Popen(del_cmd, shell=True, stdout=subprocess.PIPE)
        pp.communicate()


s3 = boto3.client('s3')
resp = s3.list_objects_v2(Bucket='fw-pg-datascience', Prefix='Walmart SOV/')

print(resp)
for obj in resp['Contents']:

    if 'csv000' in obj:
        f_name=obj['Key']
        new_f_name=f_name.split('.', 1)[0] + '.csv'
        print(new_f_name)
        #print(obj)

        s3 = boto3.resource('s3')

        print("Copying " + f_name + " To " + new_f_name)
        rename_cmd = 'aws s3 cp \"s3://fw-pg-datascience/' + f_name + '\" \"s3://fw-pg-datascience/' + new_f_name + '\"'

        del_cmd = 'aws s3 rm \"s3://fw-pg-datascience/' + f_name + '\"'

        p = subprocess.Popen(rename_cmd, shell=True, stdout=subprocess.PIPE)
        p.communicate()

        print("Deleting Old " + f_name  )
        pp = subprocess.Popen(del_cmd, shell=True, stdout=subprocess.PIPE)
        pp.communicate()



s3 = boto3.client('s3')
resp = s3.list_objects_v2(Bucket='fw-pg-datascience', Prefix='Walmart CA SOV/')

print(resp)
for obj in resp['Contents']:

    if 'csv000' in obj:

        f_name=obj['Key']
        new_f_name=f_name.split('.', 1)[0] + '.csv'
        print(new_f_name)
        #print(obj)

        s3 = boto3.resource('s3')

        print("Copying " + f_name + " To " + new_f_name)
        rename_cmd = 'aws s3 cp \"s3://fw-pg-datascience/' + f_name + '\" \"s3://fw-pg-datascience/' + new_f_name + '\"'

        del_cmd = 'aws s3 rm \"s3://fw-pg-datascience/' + f_name + '\"'

        p = subprocess.Popen(rename_cmd, shell=True, stdout=subprocess.PIPE)
        p.communicate()

        print("Deleting Old " + f_name  )
        pp = subprocess.Popen(del_cmd, shell=True, stdout=subprocess.PIPE)
        pp.communicate()



print('done unloading and renaming files')







'''
path = 's3://fw-sov/'
uploadUrl = 's3://fw-sov/Shipt-DSCopy/'

#Make Copies of the Files

s3 = boto3.client('s3')
all_files = s3.list_objects_v2(Bucket='fw-sov', Prefix='Shipt-DS')

print(all_files)

for obj in all_files['Contents']:
    keys=obj['Key']

    full_file_path= path+keys

    print(obj)
    print(keys)
    print(full_file_path)

    now = datetime.datetime.now()

    full_sql = fw_sql.format(full_file_path)

    qr = connect_to_fw_db.execute(text(full_sql))

    print("Copying "+keys+" To Shipt-DSCopy ")
    cmd = 'aws s3 cp '+path+keys+' '+uploadUrl
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    p.communicate()

'''
