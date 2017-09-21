#! /usr/bin/env python

import datetime, calendar, time, math, boto3
import os
from hurry.filesize import size
from dateutil.tz import tzlocal
from operator import attrgetter

# return the region for given bucket name
def get_location(client, name):
    return client.get_bucket_location(Bucket=name)['LocationConstraint']

# Get number of flies from the objectlist, create a list of last modified dates in unux epoch from the objects and return the latest in string
def get_objectinfo(objtlist):
    totalsize = 0 
    if objtlist:
        lastmodified = []
        for item in objtlist:
            totalsize += item['Size']
            lastmodified.append(int(calendar.timegm(item['LastModified'].utctimetuple())))
        lastmodified = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(max(lastmodified)))
    return lastmodified, size(totalsize)

# print out the list of buckets with the relevant information
def print_list(blist):
    print "Region, Date Created, Number of Files, Total File Size, Last Modified, Bucket Name"
    for each in blist:
        print each.location, each.creationdate, each.numoffiles, each.totalfilesize, each.lastmodified, each.name

#get a list of objects from the bucket
def getlist(input):
    client, bucketname, prefix = input
    mylist, nextmarker = [], ''
    if prefix=='':
        templist = client.list_objects_v2(Bucket=bucketname)
        mylist.extend(templist['Contents'])
    else:
        templist = client.list_objects_v2(Bucket=bucketname, Prefix=prefix)
        if 'Contents' in templist.keys():
            mylist.extend(templist['Contents'])
    end = templist['IsTruncated']
    while end:
        nextmarker = templist['NextContinuationToken']
        templist = client.list_objects_v2(Bucket=bucketname, ContinuationToken=nextmarker, Prefix=prefix)
        mylist.extend(templist['Contents'])
        end = templist['IsTruncated']
#        print prefix+ " ", str(len(mylist)), mylist[-1]['Key']
    return mylist

if __name__ == "__main__":
    import argparse, boto3
    from botocore import UNSIGNED
    from botocore.client import Config

    parse = argparse.ArgumentParser(description="s3bucket tool")
    parse.add_argument("-b", "--bucket", dest="bucketname", required=True,  help="Bucket name")
    parse.add_argument("-p", "--prefix", dest="prefix", default='', help="bucket prefix")
    parse.add_argument("-a", "--anon", dest="anon", default='True', help="set client to anonymous") 
    parse.add_argument("-s", "--sorted", dest="sorted", default='False', help="sort objects according to storage class")
    parse.add_argument("-id", dest="credfile", default="cred.json", help="json file containing the AWS credentials")

    args = parse.parse_args()
    print args
    
#log in s3 client, sorted is disabled when using anonymous login
    if args.anon == 'True':
        args.sorted='False'
        try:
            s3 = boto3.client(service_name='s3', config=Config(signature_version=UNSIGNED))
        except:
            print("Unable to connect to S3 Service using annonymous client")
            exit()
    else:
        import json
        try:
            cred = json.loads(open(args.credfile).read())
            cred['API'], cred['secret']
        except:
            print("Unable to load credentials") 
            exit()
        try:
            s3 = boto3.client(service_name='s3', aws_access_key_id= cred['API'], aws_secret_access_key= cred['secret'])
        except:
            print("Unable to connect to S3 Service via AWS credentials")
            exit()

#fetch objects from the bucket
    bucketname = args.bucketname
    mylist = getlist((s3, args.bucketname, args.prefix))

#sort if required
    if args.sorted=="True":
        mylist.sort(key=lambda x: x['StorageClass'], reverse=False)

#display output
    print "File Name, File Size, Storage Class"
    print "==================================="
    for each in mylist:
        print each['Key']+ ", "+size(each['Size']) + ", " + each['StorageClass']
    print "Last Modified: " + get_objectinfo(mylist)[0] + " Total Size: " + get_objectinfo(mylist)[1]
    print "Bucket Name: " + args.bucketname + " Total Number of files: " + str(len(mylist))
