#!/usr/bin/env python

"""s3_to_sdb.py: Simple Python script for restoring SimpleDB domains from S3 backups"""
__author__ = "dcarney / http://github.com/dcarney"
__email__ = "dcarney@gmail.com"

import codecs
import cPickle as pickle
import pickletools
from datetime import datetime
import math
import os
import sys
from time import strftime
import traceback

import argparse
import boto
from boto.s3.key import Key

def format_bytes(bytes):
    """Formats a number of bytes into a string that's more human-readable.
       Ex. format_bytes(124023) => 121kb
           format_bytes(6920610) => 6.6Mb
    """
    if (bytes >= 1048576):
        return "{0:0.1f}Mb".format(bytes / 1048576.0)
    elif (bytes >= 1024):
        return "{0}kb".format(int(math.floor(bytes / 1024)))
    else:
        return "{0}b".format(bytes)

def s3_progress_callback(bytes_complete, bytes_total):
    """Callback for displaying S3 download progress"""
    sys.stdout.write("{0} of {1} transferred\n".format(format_bytes(bytes_complete),
                                                       format_bytes(bytes_total)))
    sys.stdout.flush()

def download_from_s3(s3_conn, s3_bucket, s3_key, filename):
    """Saves a file locally from s3, using the supplied s3 connection, bucket, and key"""
    bucket = s3_conn.get_bucket(s3_bucket)
    k = bucket.get_key(s3_key)
    
    with codecs.open(filename, 'wb') as f:
        k.get_file(f, cb=s3_progress_callback, num_cb=10)

def sdb_domain_exists(sdb_conn, domain_name):
    try:
        sdb_conn.get_domain(domain_name, validate=True)
        return True
    except boto.exception.SDBResponseError as sdb_ex:
        return False
    
def restore_to_sdb(sdb_conn, sdb_items, domain_name):
    """Saves a set of items to a SimpleDB domain, creating the domain first if necessary"""
    
    domain = None
    if (not sdb_domain_exists(sdb_conn, domain_name)):
        domain = sdb_conn.create_domain(domain_name)
    else:
        domain = sdb_conn.get_domain(domain_name, validate=False)
    
    for item in sdb_items:
        domain.new_item(unicode(item)) # item = "GUID"
        dict = sdb_items[item]
        
        if (not domain.put_attributes(unicode(item), dict, replace=True)):
            print "Failed to save one or more attributes to item", item
            return False
            
    return True
    
def unpickle_domain(filename):
    """Unpickles and returns a dict of boto SimpleDB items"""
    
    with codecs.open(filename, 'rb') as f:
        return pickle.load(f)

def main():
    try:
        # Command-line argument/option parsing
        parser = argparse.ArgumentParser(description="Restore a SimpleDB domain from a backup on S3")
        parser.add_argument('accessKey', help="AWS Access Key")
        parser.add_argument('secretKey', help="AWS Secret Key")
        parser.add_argument('bucket', help="The S3 bucket to read from")
        parser.add_argument('key', help="The S3 key decribing the file to read from. Ex: foo/bar/Foobar_20110707T134911")
        parser.add_argument('domain_name', help='A SimpleDB domain name to restore.')
        arguments = parser.parse_args()
        
        # some simple input cleansing
        s3_key = arguments.key.strip()     
        s3_bucket = arguments.bucket.replace('/', '').strip()
        domain_name = arguments.domain_name.strip()		
            
        # setup Unicode support for stdout and stderr
        sys.stderr = codecs.getwriter('utf8')(sys.stderr)
        sys.stdout = codecs.getwriter('utf8')(sys.stdout)
        
        sdb_conn = boto.connect_sdb(arguments.accessKey, arguments.secretKey)
        s3_conn = boto.connect_s3(arguments.accessKey, arguments.secretKey)
        
        print "Downloading", s3_key, "from S3..."
        local_filename = os.path.basename(s3_key)
        download_from_s3(s3_conn, s3_bucket, s3_key, local_filename)
        print "Unpacking", local_filename, "..."
        sdb_items = unpickle_domain(local_filename)
        os.remove(os.path.join(os.getcwd(), local_filename))
        
        print "Restoring items to the", domain_name, "domain..."
        if (restore_to_sdb(sdb_conn, sdb_items, domain_name)):
            print "Complete.", len(sdb_items), "items restored."
        else:
            print "One or more errors occurred while saving SimpleDB items"
            sys.exit(-1)
            
    except Exception as ex:
        print type(ex), ex
        traceback.print_exc(file=sys.stdout)
       	sys.exit(-1)

if __name__ == "__main__":
    main()
   
   