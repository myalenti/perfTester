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
import names
import random
from faker import Factory
from collections import OrderedDict

from pymongo import MongoClient, InsertOne

logging.basicConfig(level=logging.INFO,
                    format='(%(threadName)4s) %(levelname)s %(message)s',
                    )
def usage():
    print "Performance tester usage:"
    print "     -c,--counter <number of records to insert> default is 10k"
    print "     -p,--process <number of processes to spawn> default is 1"
    print "     -C <collection Name> for documents to be loaded into"
    print "     -D <DB Name> to use to load data"
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
    print "     -r <replicaSetName> Name of replica set to connect to"
    print "     -m issue find requests - incomplete - testing phase "
    print "     -T doc type 1,2"

    

try:
    opts, args = getopt.getopt(sys.argv[1:], "R:c:C:D:p:dhboms:r:U:P:t:x:T:", ["counter=", "process=", "level="])
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
global tdb
tdb="testDBx"
global tcoll
tcoll = "test"
global repSet
repSet=""
global retry_count
retry_count=10
global retries
retries=0
findOn=False
docType=1

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
    elif opt in ("-D"):
        print "Database set to: " , arg
        tdb=str(arg)
    elif opt in ("-R"):
        print "Replica Set name  set to: " , arg
        repSet=str(arg)
    elif opt in ("-C"):
        print "Collection set to: " , arg
        tcoll=str(arg)
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
    elif opt in ("-T"):
        docType=int(arg)
    elif opt in ("-t"):
    	print "Target is set to: ", arg
    	#global target 
    	target = arg
    elif opt in ("-m"):
        print "Issuing Find Commands"
        findOn = True
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
    

       

quotient = total_records/process_count



logging.debug("Your initialization variables are as follows: ")
logging.debug("bulk : %r"  % bulk)
logging.debug("Total Records : %d"  % total_records)
logging.debug("Process Count : %d"  % process_count)
logging.debug("bulkSize : %d" % bulkSize )
logging.debug("padSize : %d"  % padSize)
logging.debug("message : %f bytes" % sys.getsizeof(message))
              
def connector():
	global retries
	if retries < retry_count:
    		try:
    			connection = MongoClient(target,port,replicaSet=repSet,serverSelectionTimeoutMS=2000,connectTimeoutMS=2000)
    			if username != "":
       				connection.admin.authenticate(username,password)
			retries=0
    			return connection
    		except :
			retries += 1
			print "connection failure, attempting again"
			print "Attempted " + str(retries) + " retries"
			connector()
	else:
		print "Number of connection retries exhausted, exiting"
		sys.exit()
	return


def findWorker(record_count):
    p = multiprocessing.current_process()
    logging.debug("You have entered the single threaded findWorker for the process %s" % p.name)
    start_time = time.time()
    logging.info("Starting Process %s %d at %s" % (p.name, p.pid, start_time))
    logging.info("Finding %d records" % record_count)
    
    connection = connector()
    db = connection[tdb]
    col_test = db[tcoll]
    print col_test
    findString = { "fld2" : "dolor sit amet, consetetur" }
    findString2 = { "fld_xx" : "Non si Trova" }
    findString3 = { "fld9" : 411900 }
    
    for i in xrange(record_count):
        r = col_test.find(findString)
        s = col_test.find(findString2)
        t = col_test.find(findString3)
        print str(r.count()) + " " + str(s.count()) + " " + str(t.count())
    
def worker(record_count):
    
    p = multiprocessing.current_process()
    logging.debug("You have entered the single threaded worker for the process %s"% p.name)
    start_time = time.time()
    logging.info("Starting Process %s %d at %s" % (p.name, p.pid, start_time))
    logging.info("Inserting %d records" % record_count)
    #connection = MongoClient(target,port,replicaSet=repSet)
    #if username != "":
    #    connection.admin.authenticate(username,password)
    connection = connector()
    db = connection[tdb]
    col_test = db[tcoll]
    for i in xrange(record_count):
    	try:
            myInserts = col_test.insert_one(generateDocument(docType))
            print "Write successful"
        except:
                rcounter = 0
                print "Write exception, failed"
                while connection.is_primary == False:
                	print "Waiting for client to establish a connection to new primary"
                	time.sleep(1)
                	rcounter += 1
                	if rcounter > 40:
                    		print "Connection to new primary could not be established, exiting"
                    		sys.exit()
        
                myInserts = col_test.insert_one(generateDocument(docType))

       	time.sleep(1)
    end_time = time.time()
    
    logging.info("Elapsed Time for job %s was %g seconds" % (p.name , end_time - start_time))
    
    return

def generateDocument(docType):
    faker = Factory.create()
    if docType == 1:
        randSequence = random.sample(xrange(9999),30)
        fakeText = faker.text()
        firstname = names.get_first_name()
        lastname = names.get_last_name()
        email = firstname + "." + lastname + "@mongodb.com"
        record = OrderedDict() 
        record['firstName'] = firstname
        record['lastName'] = lastname
        record['email'] = email
        record['homeAddress'] = faker.address()
        record['officeAddress'] = faker.address()
        record['age'] = random.randint(20, 72)
        record['commuteDistance'] = random.randint(2, 200)
        record['familySize'] = random.randint(2, 10)
        record['saTeamNum'] = random.randint(1, 500)
        record['telsaVin'] = faker.md5()
        record['nfld1'] = randSequence[0]
        record['nfld2'] = randSequence[2]
        record['nfld3'] = randSequence[3]
        record['nfld4'] = randSequence[4]
        record['nfld5'] = randSequence[5]
        record['nfld6'] = randSequence[6]
        record['nfld7'] = randSequence[7]
        record['nfld8'] = randSequence[8]
        record['nfld9'] = randSequence[9]
        record['nfld10'] = randSequence[10]
        record['nfld11'] = randSequence[11]
        record['nfld12'] = randSequence[12]
        record['nfld13'] = randSequence[13]
        record['nfld14'] = randSequence[14]
        record['nfld15'] = randSequence[15]
        record['nfld16'] = randSequence[16]
        record['nfld17'] = randSequence[17]
        record['nfld18'] = randSequence[18]
        record['nfld19'] = randSequence[19]
        record['nfld20'] = randSequence[20]
        record['nfld21'] = randSequence[21]
        record['nfld22'] = randSequence[22]
        record['nfld23'] = randSequence[23]
        record['nfld24'] = randSequence[24]
        record['nfld25'] = randSequence[25]
        record['nfld26'] = randSequence[26]
        record['nfld27'] = randSequence[27]
        record['nfld28'] = randSequence[28]
        record['nfld29'] = randSequence[29]
        record['nfld30'] = randSequence[1]
        record['strfld1'] = fakeText[0:20]
        record['strfld2'] = fakeText[21:40]
        record['strfld3'] = fakeText[41:60]
        record['strfld4'] = fakeText[61:80]
        record['strfld5'] = fakeText[81:100]
        record['strfld6'] = fakeText[101:120]
        record['strfld7'] = fakeText[121:140]
        record['strfld8'] = fakeText[141:160]
        record['strfld9'] = fakeText[161:180]
        record['strfld10'] = fakeText[181:200]
        
        #print record
        return record
    if docType == 2:
        record = {}
        record['pad'] = message
        #print record
        return record
        


def dropper():
    connection = MongoClient(target,port,replicaSet=repSet)
#    if username != "":
#        connection.admin.authenticate(username,password)
#    db = connection[tdb]
    connection = connector()
    db = connection[tdb]
    col = db[tcoll]
    logging.info("Dropping collection")
    col.drop()

def bulkworker(record_count, bulkSize, ord):
    
    p = multiprocessing.current_process()
    logging.debug("You have entered the bulk writer process for process %s" % p.name)
    start_time = time.time()
    logging.info("Starting Process %s %d at %s" % (p.name, p.pid, start_time))
    logging.info("Inserting %d records" % record_count)
    #connection = MongoClient(target,port,replicaSet=repSet)
    #if username != "":  
    #    connection.admin.authenticate(username,password)
    connection = connector()
    db = connection[tdb]
    col_test = db[tcoll]
    
        
    
    request = []
    i = 0
 
    while (i < record_count):
        i = i + bulkSize
        
        for r in xrange(bulkSize):
            #request.append(InsertOne({"a":"1", "b":"hello mark"}))
            request.append(InsertOne(generateDocument(docType)))
            #print request
        bulk_result = col_test.bulk_write(request,ordered=ord)
        #logging.debug("Result Dump : %s" % json.dumps(bulk_result.bulk_api_result))
        #logging.debug("Bulk Write result %d of %d" %(bulkSize, ????))
        request = []
    end_time = time.time()
    logging.info("Elapsed Time for job %s was %g seconds" % (p.name , end_time - start_time))
    
    

logging.debug("Size of record is : %f bytes" % (sys.getsizeof(generateDocument(docType))))
jobs = []


for i in range(process_count):
    if bulk == False:
        if findOn == True:
            p = multiprocessing.Process(target=findWorker, args=(quotient,))
        else:
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
 
