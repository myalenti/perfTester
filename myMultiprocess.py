import random
import threading
import time
import logging
import pymongo
import sys
import timeit
import multiprocessing
import sys
import getopt
import ast
import json
from pymongo import MongoClient, InsertOne

logging.basicConfig(level=logging.INFO,
                    format='(%(threadName)4s) %(levelname)s %(message)s',
                    )
def usage():
    print "Performance tester usage:"
    print "Required command line options: "
    print "     -c,--counter <number of records to insert> default is 10k"
    print "     -p,--process <number of processes to spawn> default is 1"
    print "Optional Options : "
    print "     -b use bulk inserts default is false"
    print "     -s <size of the bulk insert> default is 10"
    print "     -r <number of characters to pad wih> 124"
    print "     -U <db username>"
    print "     -P <db password>"
    print '     --level <DEBUG,INFO,WARNING>'
    print "     -t targetserver"
    print "     -x port"
    print "     -d drop collection after execution"
    print "     -o Use ordered bulk writes (defaults is false)"
    

try:
    opts, args = getopt.getopt(sys.argv[1:], "c:p:dhbos:r:U:P:t:x:", ["counter=", "process=", "level="])
    logging.debug("Operation list length is : %d " % len(opts))
except getopt.GetoptError:
    print "You provided invalid command line switches."
    usage()
    exit(2)


bulk = False
bulkSize = 10
total_records = 10000
process_count = 1
padSize = 124
message = ""
global record
drop=False
ord=False
global port
port=27017
global username
username=""
global password
password=""
global target
target="localhost"

for opt, arg in opts:
    #print "Tuple is " , opt, arg
    if opt in ("-d"):
        drop=True
    elif opt in ("-b"):
        bulk=True
    elif opt in ("-c" , "--counter"):
        print "Total Records set to : ", arg
        total_records = int(arg)
    elif opt in ("-p", "--process"):
        print "Total Processes set to : " , arg
        process_count = int(arg)
    elif opt in ("-s"):
        print "Bulk Bucket size set to: ", arg
        bulkSize = int(arg)
    elif opt in ("-o"):
        ord=True
    elif opt in ("-r"):
        print "Padding size set to: ", arg
        padSize = int(arg)
    elif opt in ("-U"):
        print "Username set to: ", arg
        #global username
        username = arg
    elif opt in ("-P"):
        print "Password set to: ", arg
        #global password
        password = arg
    elif opt in ("-x"):
    	print "Port is set to: ", arg
    	#global port
        port = int(arg)
    elif opt in ("-t"):
    	print "Target is set to: ", arg
    	#global target 
    	target = arg
    elif opt in ("--level"):
        print "Log Level set to : ", arg
        arg = arg.upper() 
        if not arg in ("DEBUG", "WARN", "INFO"):
            print "Invalid logging level specified"
            exit(2)
        else:
            logging.getLogger().setLevel(arg)
    elif opt in ("-h"):
        usage()
        exit()
    else:
        usage()
        exit(2)


for s in xrange(padSize):
    message += str(s)
    
record = {}
record['pad'] = message
       

quotient = total_records/process_count



logging.debug("Your initialization variables are as follows: ")
logging.debug("bulk : %r"  % bulk)
logging.debug("Total Records : %d"  % total_records)
logging.debug("Process Count : %d"  % process_count)
logging.debug("bulkSize : %d" % bulkSize )
logging.debug("padSize : %d"  % padSize)
logging.debug("message : %f bytes" % sys.getsizeof(message))
              
def worker(record_count):
    
    p = multiprocessing.current_process()
    logging.debug("You have entered the single threaded worker for the process %s"% p.name)
    start_time = time.time()
    logging.info("Starting Process %s %d at %s" % (p.name, p.pid, start_time))
    logging.info("Inserting %d records" % record_count)
    connection = MongoClient(target,port)
    connection.admin.authenticate(username,password)
    db = connection.testDB
    col_test = db.test
    for i in xrange(record_count):
        #myInserts = col_test.insert_one({"a":"1", "b":"hello mark"})
        #myInserts = col_test.insert_one(record)
        #del record['_id']
        myInserts = col_test.insert_one({"pad": record['pad']})
        time.sleep(1)
    end_time = time.time()
    
    logging.info("Elapsed Time for job %s was %g seconds" % (p.name , end_time - start_time))
    
    
    return
def dropper():
    connection = MongoClient(target,port)
    if username != "":
        connection.admin.authenticate(username,password)
    db = connection.testDB
    col_test = db.test
    logging.info("Dropping collection")
    col_test.drop()

def bulkworker(record_count, bulkSize, ord):
    
    p = multiprocessing.current_process()
    logging.debug("You have entered the bulk writer process for process %s" % p.name)
    start_time = time.time()
    logging.info("Starting Process %s %d at %s" % (p.name, p.pid, start_time))
    logging.info("Inserting %d records" % record_count)
    connection = MongoClient(target,port)
    if username != "":  
        connection.admin.authenticate(username,password)
    db = connection.testDB
    col_test = db.test
    
        
    
    request = []
    i = 0
 
    while (i < record_count):
        i = i + bulkSize
        
        for r in xrange(bulkSize):
            #request.append(InsertOne({"a":"1", "b":"hello mark"}))
            request.append(InsertOne({"pad": record['pad']}))
            #print request
        bulk_result = col_test.bulk_write(request,ordered=ord)
        #logging.debug("Result Dump : %s" % json.dumps(bulk_result.bulk_api_result))
        #logging.debug("Bulk Write result %d of %d" %(bulkSize, ????))
        request = []
    end_time = time.time()
    logging.info("Elapsed Time for job %s was %g seconds" % (p.name , end_time - start_time))
    
    

logging.debug("Size of record is : %f bytes" % (sys.getsizeof(record['pad'])))
jobs = []
for i in range(process_count):
    if bulk == False: 
        p = multiprocessing.Process(target=worker, args=(quotient,))
        jobs.append(p)
    else:
        p = multiprocessing.Process(target=bulkworker, args=(quotient,bulkSize,ord))
        jobs.append(p)
    p.start() 

main_process = multiprocessing.current_process()
logging.debug('Main process is %s %s' % (main_process.name,main_process.pid))

for i in jobs:
    i.join()
   
if drop == True:
	dropper()
 
