import boto, threading, Queue, sys, traceback, os, argparse
from boto.s3.key import Key
desc = """
Created By: Chris Hayes
Version:    0.0
Date:       20110715
Notes:

"""
parser = argparse.ArgumentParser(description=desc)
parser.add_argument('-a', action="store", dest="accessKey", help="AWS Access Key")
parser.add_argument('-s', action="store", dest="secretKey", help="AWS Secret Key")
parser.add_argument('-b', action="store", dest="bucket", help="name of bucket to write to")
arguments = parser.parse_args()


def percent_cb(complete, total):
    sys.stdout.write("{0}.{1}\n".format(complete,total))
    sys.stdout.flush()

def formatExceptionInfo(maxTBlevel=5):
    cla, exc, trbk = sys.exc_info()
    excName = cla.__name__
    try:
        excArgs = exc.__dict__["args"]
    except KeyError:
        excArgs = "<no args>"
    excTb = traceback.format_tb(trbk, maxTBlevel)
    return (excName, excArgs, excTb)

def bucketExists(self,bucketname,s3Connection):
    return True

def domainExists(self,domainname,sdbConnection):
    return True

def backupDomain(bucketname,domainKeys,bucketKeys, threadcount = 1):
    if isinstance(domainKeys,environmentKeys) and isinstance(bucketKeys,environmentKeys):
        queue = Queue.Queue()
        for i in range(threadcount):
            t = threadSDBArchive(queue,bucketname,bucketKeys,domainKeys)
            t.setDaemon(True)
            t.start()
        conn = boto.connect_sdb(domainKeys.ACCESSKEY, domainKeys.SECRETKEY)
        domains = conn.get_all_domains()
        for d in domains:
            queue.put(d.name)
        
        #queue.initQueue()
        
        queue.join()

class environmentKeys:
    def __init__(self,accessKey,secretKey):
        self.ACCESSKEY = accessKey
        self.SECRETKEY = secretKey

class threadSDBArchive(threading.Thread):
    def __init__(self, queue,bucket,s3keys,sdbkeys):
        threading.Thread.__init__(self)
        self.queue = queue
        self.S3KEYS = s3keys
        self.SDBKEYS = sdbkeys
        self.BUCKETNAME = bucket
        self.S3CONNECTION = None
        self.SDBCONNECTION = None
        self.DOMAIN = None
                
    def run(self):
        while True:
            domain = self.queue.get()
            self.connect()
            self.loaddomaintos3(domain)
            self.queue.task_done()
            
    def connect(self):
        if self.S3CONNECTION == None:
            self.S3CONNECTION = boto.connect_s3(self.S3KEYS.ACCESSKEY, self.S3KEYS.SECRETKEY)
            self.BUCKET = self.S3CONNECTION.get_bucket(self.BUCKETNAME)
        if self.SDBCONNECTION == None:
            self.SDBCONNECTION = boto.connect_sdb(self.SDBKEYS.ACCESSKEY, self.SDBKEYS.SECRETKEY)
              
    def loaddomaintos3(self, domainname):
        domain = self.SDBCONNECTION.get_domain(domainname)
        for itm in domain:
            k = Key(self.BUCKET)
            k.key = "{0}/{1}/{2}".format("simpledbBU",domainname,itm.name)
            txt = ""
            for k,v in itm:
                if isinstance(v,basestring):
                    txt += "|{0}=\"{1}\"".format(k,v)
                else:
                    for val in v:
                        txt += "|{0}=\"{1}\"".format(k,val)
            k.set_contents_from_string(txt.lstrip('|'))  
    
def domainQueue(keys):
    queue = Queue.Queue()
    conn = boto.connect_sdb(keys.ACCESSKEY, keys.SECRETKEY)
    domains = conn.get_all_domains()
    for d in domains:
        queue.put(d.name)
    return queue
#class domainQueue(Queue.Queue):
#    def __init__(self,sdbkeys):
#        super(domainQueue,self).__init__()
#        self.SDBKEYS = sdbkeys
#
#    def initQueue(self):
#        conn = boto.connect_sdb(self.SDBKEYS.ACCESSKEY, self.SDBKEYS.SECRETKEY)
#        domains = conn.get_all_domains()
#        for d in domains:
#            self.put(d.name)

def main():
    envkeys = environmentKeys(arguments.accessKey,arguments.secretKey)
    backupDomain(arguments.bucket,envkeys,envkeys)

if __name__ == "__main__":main()