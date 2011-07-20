#!/usr/bin/env python

"""sdb2s3.py: Simple Python script for backing up/saving SimpleDB domains to S3"""
__author__ = "dcarney / http://github.com/dcarney"
__email__ = "dcarney@gmail.com"

import argparse
import codecs
import cPickle as pickle
from datetime import datetime
import math
import os
import sys
from time import strftime
import traceback

import boto
from boto.s3.key import Key

def format_bytes(bytes):
    """Formats a number of bytes into a string that's more human-readable.
       Ex. format_bytes(124023) => 121kb
    """
    if (bytes >= 1024):
        return str(int(math.floor(bytes / 1024))) + "kb"
    else:
        return str(bytes) + "b"

def s3_progress_callback(bytes_complete, bytes_total):
    """Callback for displaying S3 upload progress"""
    sys.stdout.write("{0} of {1} transferred\n".format(format_bytes(bytes_complete),
                                                       format_bytes(bytes_total)))
    sys.stdout.flush()

def save_to_s3(s3_conn, s3_bucket, s3_prefix, filename):
    """Saves a local file <filename> to S3, using the supplied s3 connection,
       bucket, and prefix"""
    bucket = s3_conn.get_bucket(s3_bucket)
    k = Key(bucket)
    k.key = "{0}{1}".format(s3_prefix if s3_prefix.endswith('/') else s3_prefix + '/', filename)
    k.set_contents_from_filename(filename, cb=s3_progress_callback, num_cb=10, replace=True)

def pickle_domain(sdb_items, domain_name):
    """Pickles a set of boto SimpleDB items, using the pickle protocol #2 for size,
    and speed, returning the filename of the resulting pickle."""
    item_dict = {}
    for item in sdb_items:
        for k in item:
            item_dict[item.name] = { k : item[k] }
    
    # build a sensible, UTC-timestamped filename, eg: SomeDomain_2011-07-20T22_15_27_839618
    filename = (domain_name + '_' + datetime.isoformat(datetime.utcnow())).replace(':', '_').replace('.', '_')
    
    with codecs.open(filename, 'wb') as f:
        # protocols > 0 write binary
        # in terms of speed: protocol 2 > 1 > 0
        # specifying a protocol version < 0 selects the highest supported protocol
        pickle.dump(item_dict, f, 2)
    
    return filename

def main():
    try:
        # Command-line argument/option parsing
        parser = argparse.ArgumentParser(description="Backup a SimpleDB domain to S3")
        parser.add_argument('accessKey', metavar="A", help="AWS Access Key")
        parser.add_argument('secretKey', metavar="S", help="AWS Secret Key")
        parser.add_argument('bucket', metavar="B", help="The S3 bucket to write to")
        parser.add_argument('prefix', metavar="P", help="The S3 prefix to write to. Ex: some/crazy/prefix")
        parser.add_argument('--name', 
                            dest='domain_name',
                            help='The name of the domain to backup.  If none is given, all domains ' + 
                                 'that are accessible using the given keys are backed up.')
        arguments = parser.parse_args()
        
        # some simple input cleansing
        s3_prefix = arguments.prefix
        if (not s3_prefix.endswith('/')):
            s3_prefix = s3_prefix + '/'
            
        s3_bucket = arguments.bucket.replace('/', '')      
            
        # setup Unicode support for stdout and stderr
        sys.stderr = codecs.getwriter('utf8')(sys.stderr)
        sys.stdout = codecs.getwriter('utf8')(sys.stdout)
        
        sdb_conn = boto.connect_sdb(arguments.accessKey, arguments.secretKey)
        s3_conn = boto.connect_s3(arguments.accessKey, arguments.secretKey)
        
        if arguments.domain_name is not None:
            try:
                domains = [ sdb_conn.get_domain(arguments.domain_name, validate=True) ]
            except boto.exception.SDBResponseError:
                print "No domain named ", arguments.domain_name, "exists."
                sys.exit(-1)
        else:
            # If no domain was given, do all the available domains
            domains = sdb_conn.get_all_domains()
        
        for domain in domains:
            print "\nReading", domain.name, "..."
            filename = pickle_domain(domain, domain.name)
            print "Saving", domain.name, "to S3..."
            save_to_s3(s3_conn,
                       s3_bucket,
                       "{0}{1}/".format(s3_prefix, domain.name),
                       filename)
            # delete the local file
            os.remove(os.path.join(os.getcwd(), filename))
            
    except Exception as ex:
        print type(ex), ex
        traceback.print_exc(file=sys.stdout)
       	sys.exit(-1)

if __name__ == "__main__":
    main()
   
   