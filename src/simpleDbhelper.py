import boto, threading, Queue, sys, traceback, os, argparse, datetime
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

def saveFileToS3(bucketname, domainname, filename, s3keys):
    s3conn = boto.connect_s3(s3keys.ACCESSKEY, s3keys.SECRETKEY)
    bucket = s3conn.get_bucket(bucketname)
    k = Key(bucket)
    k.key = "{0}/{1}.xml".format(datetime.date.today().strftime("%Y%m%d"),domainname)
    k.set_contents_from_filename(filename, cb=percent_cb, replace=True)

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

def restoreValue(s3bucketConnection, sdbDomainConnection, guid):
    #read val
    #write val
    print 'not implemented'

def findBUVal(bucket,budir,domainname,val):
    key = bucket.get_key("{0}/{1}.xml".format(budir,domainname))
    node = re.compile("<item .*{0}.*/>".format(val))
    point = 0
    x =""
    vals = []
    while point < key.size:
        x += key.read(key.BufferSize)
        vals += node.findall(x)
        x = x.splitlines()[len(x.splitlines())-1]
        point += key.BufferSize
    return vals

def convertNodeToDict(node):
    vals = xml.parseString(node, parser=None)
    
    return 'not implemented'

def restoreVals(vals,domain):
    for v in vals:
        print 'not implemented'

def findDomainVal(domain, val):
    return 'not implemented'

def writeDomainVal(val, domain):
    domain.new_item()
    print 'not implemented'

def restoreDomain(bucketname,budir,domainKeys,bucketKeys, domainname):
    s3conn = boto.connect_s3(bucketKeys.ACCESSKEY, bucketKeys.SECRETKEY)
    sdbconn = boto.connect_sdb(domainKeys.ACCESSKEY, domainKeys.SECRETKEY)
    bucket = s3conn.get_bucket(bucketname)
    domain = sdbconn.get_domain(domainname)
    #keys = bucket.list(budir)
    key = bucket.get_key("{0}/{1}.xml".format(budir,domainname))
    point = 0
    while point < key.size:
        x = key.read(key.BufferSize)
        for l in x.splitlines():
            print l
        #print x[:5]
        
        point += key.BufferSize

        
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
        self.DOMAINNAME = ""
        self.FILENAME = ""
                
    def run(self):
        while True:
            self.DOMAINNAME = self.queue.get()
            self.connect()
            self.createStagingFile()
            self.saveFileToS3()
            self.queue.task_done()
            
    def connect(self):
        if self.S3CONNECTION == None:
            self.S3CONNECTION = boto.connect_s3(self.S3KEYS.ACCESSKEY, self.S3KEYS.SECRETKEY)
            self.BUCKET = self.S3CONNECTION.get_bucket(self.BUCKETNAME)
        if self.SDBCONNECTION == None:
            self.SDBCONNECTION = boto.connect_sdb(self.SDBKEYS.ACCESSKEY, self.SDBKEYS.SECRETKEY)
    
    def saveFileToS3(self):
        k = Key(self.BUCKET)
        k.key = "{0}/{1}.xml".format(datetime.date.today().strftime("%Y%m%d"),self.DOMAINNAME)
        k.set_contents_from_filename(self.FILENAME, cb=percent_cb, replace=True)
    
    def createStagingFile(self):
        self.DOMAIN = self.SDBCONNECTION.get_domain(self.DOMAINNAME)
        self.FILENAME = self.DOMAINNAME + ".xml"
        with open(self.FILENAME, "w") as  f:
            f.write( "<?xml version=\"1.0\" ?>" )
            f.write( "<items>" )
            for itm in self.DOMAIN:
                buffer=[]
                buffer.append('itemName="{0}"'.format(itm.name))
                for k,v in itm.items():
                    if isinstance(v,basestring):
                        txt= '{0}="{1}"'.format(k,unicode(v).encode('utf8'))
                    else:
                        for i, val in enumerate(v):
                            txt= '{0}::{1}="{2}"'.format(k,i,unicode(val).encode('utf8'))
                    buffer.append(txt)
                f.write( "  <item {0} />\n".format(" ".join(buffer)))
            f.write("</items>")
    
def domainQueue(keys):
    queue = Queue.Queue()
    conn = boto.connect_sdb(keys.ACCESSKEY, keys.SECRETKEY)
    domains = conn.get_all_domains()
    for d in domains:
        queue.put(d.name)
    return queue

def main():
    envkeys = environmentKeys(arguments.accessKey,arguments.secretKey)
    backupDomain(arguments.bucket,envkeys,envkeys, 5)

if __name__ == "__main__":main()