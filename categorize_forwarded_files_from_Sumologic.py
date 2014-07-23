# --------------------------------------------------
# Author: Binh Nguyen 
# Email: "binh@misfitwearables.com" or ntbinhptnk@gmail.com
# Feel free to ask me any question.
# --------------------------------------------------
# Description:
# When working with Sumologic, one usually uses the feature "Data Forwarding" for backing log data up.
# For example:
#   + Data Forwarding
#       Data Forwarding is active and uploading to your AWS S3 bucket.
#       Bucket Name: my_sumologic_logs
#       Description: Forward all logs from all collectors to your AWS S3 bucket.
#       Status Active
#   + Each log file forwarded to the bucket "my_sumologic_logs" has the following name format:
#       1393151887000-1393151889000--9223372036853041134.csv.gz
#   + How can we categorize these logs:
#         - Firstly, we can group all log files by day. For example, all log files from the day 2014/07/31 will store in a folder "2014-07-31"
#         - Secondly, we will unzip them and store them in another folder named "unzipped_logs/2014-07-31"

import boto
import pymongo
import traceback
import sys
import bson
from datetime import *
import os
from collections import OrderedDict
from datetime import datetime, timedelta
from optparse import OptionParser
import re
import shutil
import getopt
import ast
import gzip
import ast
import os.path
import datetime
from boto.s3.connection import S3Connection
import StringIO
import cStringIO
import gzip
from boto.s3.key import Key

S3_ACCESS_KEY="your_S3_access_key"
S3_SECRET_KEY="your_S3_secret_key"

def parsing_forwarded_filename(filename):
    return re.findall(r'\b\d+\b', filename)

def date_info(filepath):
    # Input: s3://my_sumologic_logs/1393151887000-1393151889000--9223372036853041134.csv.gz -> Output: from timestamp 1393151887000 -> Date ?
    numeric_words=parsing_forwarded_filename(filepath)
    size=len(numeric_words)
    return datetime.datetime.fromtimestamp(int(numeric_words[size-3])/1000).strftime('%Y-%m-%d')

def copy_file(src_bucket_name,dst_bucket_name,filekey,newfilekey,connection):
    src_bucket = connection.get_bucket(src_bucket_name)
    dst_bucket = connection.get_bucket(dst_bucket_name)
    dst_bucket.copy_key(newfilekey, src_bucket_name, filekey)

def move_file(src_bucket_name,dst_bucket_name,filekey,newfilekey,connection):
    src_bucket = connection.get_bucket(src_bucket_name)
    dst_bucket = connection.get_bucket(dst_bucket_name)
    dst_bucket.copy_key(newfilekey, src_bucket_name, filekey)
    src_bucket.delete_key(filekey)

def extract_filename(filepath):
    numeric_words=parsing_forwarded_filename(filepath)
    size=len(numeric_words)
    return ''.join([numeric_words[size-3],'-',numeric_words[size-2],'--',numeric_words[size-1],'.csv.gz'])

def extract_unzipped_filename(filepath):
    numeric_words=parsing_forwarded_filename(filepath)
    size=len(numeric_words)
    return ''.join([numeric_words[size-3],'-',numeric_words[size-2],'--',numeric_words[size-1],'.csv'])

def S3_decompress(connection,src_bucket_name,srcFileKey,dst_bucket_name,dstFileKey):
    src_bucket = connection.get_bucket(src_bucket_name)
    dst_bucket = connection.get_bucket(dst_bucket_name)
    src_key=src_bucket.get_key(srcFileKey)
    f = cStringIO.StringIO()
    src_key.get_file(f)
    f.seek(0) #This is crucial
    gzf = gzip.GzipFile(fileobj=f)
    file_content = gzf.read()
    dst_key=dst_bucket.new_key(dstFileKey)
    dst_key.set_contents_from_string(file_content)
    gzf.close()
    f.close()
def check_not_empty_bucket(conn,bucket_name):
    bucket=conn.get_bucket(bucket_name)
    rs = bucket.list()
    not_empty=0
    for key in rs:
        not_empty=1
        if not_empty:
            return not_empty
    return not_empty
if __name__ == "__main__":

    source_bucket="my_sumologic_logs"
    target_bucket="my_categorized_sumologic_logs_by_day"
    uncompress_folder="unzipped_logs"
    conn = boto.connect_s3(S3_ACCESS_KEY,S3_SECRET_KEY)
    bucket = conn.lookup(source_bucket)
    #check_empty_bucket="test_empty"
    #while check_not_empty_bucket(conn,source_bucket):
    while True:
        bucket = conn.lookup(source_bucket)
        for key in bucket:
            srcFilePath=key.name
            dest_folder=''.join([uncompress_folder,"/",date_info(key.name)])
            srcFileName=extract_filename(srcFilePath)
            dest_FileKey=''.join([dest_folder,"/",extract_unzipped_filename(srcFilePath)])
            
            S3_decompress(conn,source_bucket,srcFilePath,target_bucket,dest_FileKey)
            dest_move_FileKey=''.join([date_info(key.name),"/",extract_filename(srcFilePath)])        
            move_file(source_bucket,target_bucket,srcFilePath,dest_move_FileKey,conn)
