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
from JsonDocuments import JsonDocuments

from pymongo import MongoClient, InsertOne


if __name__ == "myMultiprocess":
    print("Warning - This is intended to be run as a script, not a module.")

bulk = False
bulkSize = 10
total_records = 10000
process_count = 1
padSize = 124
message = ""
record = ""
drop=False
ord=False
port=27017
username=""
password=""
target="localhost"
tdb="testDBx"
tcoll = "test"
repSet=""
retry_count=10
retries=0
findOn=False
docType=1
jdoc = JsonDocuments()

def main():
    logging.basicConfig(level=logging.INFO,
                    format='(%(threadName)4s) %(levelname)s %(message)s',
                    )




    try:
        opts, args = getopt.getopt(sys.argv[1:], "R:c:C:D:p:dhboms:r:U:P:t:x:T:", ["counter=", "process=", "level="])
        logging.debug("Operation list length is : %d " % len(opts))
    except getopt.GetoptError:
        print ("You provided invalid command line switches.")
        usage()
        exit(2)



    for opt, arg in opts:
        #print "Tuple is " , opt, arg
        if opt in ("-d"):
            global drop
            drop=True
        elif opt in ("-b"):
            global bulk
            bulk=True
        elif opt in ("-c" , "--counter"):
            global total_records
            print( "Total Records set to : ", arg)
            total_records = int(arg)
        elif opt in ("-p", "--process"):
            global process_count
            print("Total Processes set to : " , arg)
            process_count = int(arg)
        elif opt in ("-s"):
            global bulkSize
            print( "Bulk Bucket size set to: ", arg)
            bulkSize = int(arg)
        elif opt in ("-o"):
            global ord
            ord=True
        elif opt in ("-r"):
            global padSize
            print( "Padding size set to: ", arg)
            padSize = int(arg)
        elif opt in ("-D"):
            global tdb
            print( "Database set to: " , arg)
            tdb=str(arg)
        elif opt in ("-R"):
            global repSet
            print( "Replica Set name  set to: " , arg)
            repSet=str(arg)
        elif opt in ("-C"):
            global tcoll
            print( "Collection set to: " , arg)
            tcoll=str(arg)
        elif opt in ("-U"):
            global username
            print( "Username set to: ", arg)
            #global username
            username = arg
        elif opt in ("-P"):
            global password
            print("Password set to: ", arg)
            password = arg
        elif opt in ("-x"):
            global port
            print( "Port is set to: ", arg)
            port = int(arg)
        elif opt in ("-T"):
            global docType
            docType=int(arg)
        elif opt in ("-t"):
            global target
            print( "Target is set to: ", arg)
            target = arg
        elif opt in ("-m"):
            global findOn
            print( "Issuing Find Commands")
            findOn = True
        elif opt in ("--level"):
            print( "Log Level set to : ", arg)
            arg = arg.upper()
            if not arg in ("DEBUG", "WARN", "INFO"):
                print( "Invalid logging level specified")
                exit(2)
            else:
                logging.getLogger().setLevel(arg)
        elif opt in ("-h"):
            usage()
            exit()
        else:
            usage()
            exit(2)

    global message
    for s in range(padSize):
        message += str(s)
    quotient = total_records // process_count
    testDoc = jdoc.generateDocument(docType, message)
    logging.debug("Size of record is : %f bytes" % (sys.getsizeof(testDoc)))
    # logging.debug("Size of record is : %f bytes" % (sys.getsizeof(jdoc.generateDocument(docType,message))))
    jobs = []

    for i in range(process_count):
        if bulk == False:
            if findOn == True:
                p = multiprocessing.Process(target=findWorker, args=(quotient,))
            else:
                p = multiprocessing.Process(target=worker, args=(quotient,))
                jobs.append(p)
        else:
            p = multiprocessing.Process(target=bulkworker, args=(quotient, bulkSize, ord))
            jobs.append(p)
        p.start()

    main_process = multiprocessing.current_process()
    logging.debug('Main process is %s %s' % (main_process.name, main_process.pid))

    for i in jobs:
        i.join()

    if drop == True:
        dropper()




def usage():
	print("Performance tester usage:")
	print("     -c,--counter <number of records to insert> default is 10k")
	print( "     -p,--process <number of processes to spawn> default is 1")
	print( "     -C <collection Name> for documents to be loaded into")
	print( "     -D <DB Name> to use to load data")
	print( "     -b use bulk inserts default is false")
	print( "     -s <size of the bulk insert> default is 10")
	print( "     -r <number of characters to pad wih> 124")
	print( "     -U <db username>")
	print( "     -P <db password>")
	print( '     --level <DEBUG,INFO,WARNING>')
	print( "     -t targetserver")
	print( "     -x port")
	print( "     -d drop collection after execution")
	print( "     -o Use ordered bulk writes (defaults is false)")
	print( "     -r <replicaSetName> Name of replica set to connect to")
	print( "     -m issue find requests - incomplete - testing phase ")
	print( "     -T doc type 1,2")

def connector():
	global retries
	if retries < retry_count:
		try:
			connection = MongoClient(target,port,replicaSet=repSet,serverSelectionTimeoutMS=2000,connectTimeoutMS=2000)
			if username != "":
				connection.admin.authenticate(username,password)
			retries=0
			return connection
		except:
			retries += 1
			print("connection failure, attempting again")
			print( "Attempted " + str(retries) + " retries")
			connector()
	else:
		print( "Number of connection retries exhausted, exiting")
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
    print(col_test)
    findString = { "fld2" : "dolor sit amet, consetetur" }
    findString2 = { "fld_xx" : "Non si Trova" }
    findString3 = { "fld9" : 411900 }
    
    for i in range(record_count):
        r = col_test.find(findString)
        s = col_test.find(findString2)
        t = col_test.find(findString3)
        print( str(r.count()) + " " + str(s.count()) + " " + str(t.count()))
    
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
	for i in range(record_count):
		try:
			myInserts = col_test.insert_one(jdoc.generateDocument(docType,message))
			print( "Write successful")
		except:
				rcounter = 0
				print( "Write exception, failed")
				while connection.is_primary == False:
					print( "Waiting for client to establish a connection to new primary")
					time.sleep(1)
					rcounter += 1
					if rcounter > 40:
							print( "Connection to new primary could not be established, exiting")
							sys.exit()

				myInserts = col_test.insert_one(jdoc.generateDocument(docType,message))

		time.sleep(1)
	end_time = time.time()

	logging.info("Elapsed Time for job %s was %g seconds" % (p.name , end_time - start_time))

	return

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
    balance = record_count
    while (balance > 0 ):
        
        if balance > bulkSize:
            balance = balance - bulkSize
            batchSize = bulkSize
        else:
            batchSize = balance
            balance = 0
                
        for r in range(batchSize):
            request.append(InsertOne(jdoc.generateDocument(docType,message)))
            
        try : 
            bulk_result = col_test.bulk_write(request,ordered=ord)
        except pymongo.errors.BulkWriteError as e:
            #e = sys.exc_info()[0]
            print( e.details)
            #attrs = vars(e)
            #print ', '.join("%s: %s" % item for item in attrs.items())
            #print e
            exit()
        #logging.debug("Result Dump : %s" % json.dumps(bulk_result.bulk_api_result))
        #logging.debug("Bulk Write result %d of %d" %(bulkSize, ????))
        request = []
    end_time = time.time()
    logging.info("Elapsed Time for job %s was %g seconds" % (p.name , end_time - start_time))

if __name__ == "__main__":
    main()