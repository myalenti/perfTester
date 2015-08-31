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
from pymongo import MongoClient, InsertOne

def usage():
    print "Performance tester usage:"
    print "Required command line options: "
    print "     -c,--counter <number of records to insert> default is 10k"
    print "     -p,--process <number of processes to spawn> default is 1"
    print "Optional Options : "
    print "     -b use bulk inserts default is false"
    print "     -s <size of the bulk insert> default is 10"
    print "     -r <number of characters to pad wih> 124"
    

try:
    #print "Parsing command line"
    opts, args = getopt.getopt(sys.argv[1:], "c:p:hbs:r:", ["counter=", "process="])
    #print "Operation list length is : ", len(opts)
except getopt.GetoptError:
    print "You provided invalid command line switches."
    usage()
    exit(2)

#print len(sys.argv[1:])
bulk = False
bulkSize = 10
total_records = 10000
process_count = 1
padSize = 124
message = ""
global record


for opt, arg in opts:
    #print "Tuple is " , opt, arg
    if opt in ("-b"):
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
    elif opt in ("-r"):
        print "Padding size set to: ", arg
        padSize = int(arg)
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
#print record        

quotient = total_records/process_count

logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )

def worker(record_count):
    p = multiprocessing.current_process()
    start_time = time.time()
    print "Starting job " , p.name, " ", p.pid,  "at " , start_time
    print "Inserting " , record_count, " records"
    connection = MongoClient('localhost',27017)
    db = connection.testDB
    col_test = db.test
    for i in xrange(record_count):
        #myInserts = col_test.insert_one({"a":"1", "b":"hello mark"})
        #myInserts = col_test.insert_one(record)
        #del record['_id']
        myInserts = col_test.insert_one({"pad": record['pad']})
        time.sleep(1)
    end_time = time.time()
    print("Elapsed Time for job %s was %g seconds" % (p.name , end_time - start_time))
    
    
    return

def bulkworker(record_count, bulkSize):
    
    p = multiprocessing.current_process()
    start_time = time.time()
    print "Starting bulk job " , p.name, " ", p.pid,  "at " , start_time
    print "Inserting " , record_count, " records"
    connection = MongoClient('localhost',27017)
    db = connection.testDB
    col_test = db.test
    request = []
    i = 0
 #   print "initla value of i ", i
    while (i < record_count):
        i = i + bulkSize
        #print i
        for r in xrange(bulkSize):
            #request.append(InsertOne({"a":"1", "b":"hello mark"}))
            request.append(InsertOne({"pad": record['pad']}))
            #print request
        col_test.bulk_write(request)
        request = []
    end_time = time.time()
    print("Elapsed Time for job %s was %g seconds" % (p.name , end_time - start_time))
    
    
    return

print "Size of record is : %f bytes" % (sys.getsizeof(record['pad']))
jobs = []
for i in range(process_count):
    if bulk == False: 
        p = multiprocessing.Process(target=worker, args=(quotient,))
        jobs.append(p)
    else:
        p = multiprocessing.Process(target=bulkworker, args=(quotient,bulkSize,))
        jobs.append(p)
    p.start() 

main_process = multiprocessing.current_process()
print 'Main process is ' , main_process.name, " " , main_process.pid

for i in jobs:
    i.join()
    