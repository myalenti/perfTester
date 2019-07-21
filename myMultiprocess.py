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

logging.basicConfig(level=logging.INFO,
                    format='(%(threadName)4s) %(levelname)s %(message)s',
                    )


def main():
    config = OrderedDict()
    faker = Factory.create()
    config["faker"] = faker
    config["bulk"] = False
    config["bulkSize"] = 10
    config["total_records"] = 10000
    config["process_count"] = 1
    config["padSize"] = 124
    config["message"] = ""
    config["record"]= ""
    config["drop"] = False
    config["ord"]= False
    config["port"] = 27017
    config["username"] = ""
    config["password"] = ""
    config["target"] = "localhost"
    config["tdb"] = "testDBx"
    config["tcoll"] = "test"
    config["repSet"] = ""
    config["retry_count"] = 10
    config["retries"] = 0
    config["findOn"] = False
    config["docType"] = 1
    config["jdoc"] = JsonDocuments()
    jdoc = config["jdoc"]


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
            print("Configuring for drop after workload")
            config["drop"]=True
        elif opt in ("-b"):
            print("Enabling bulk Operations")
            config["bulk"]=True
        elif opt in ("-c" , "--counter"):
            print( "Total Records set to : ", arg)
            config["total_records"] = int(arg)
        elif opt in ("-p", "--process"):
            print("Total Processes set to : " , arg)
            config["process_count"] = int(arg)
        elif opt in ("-s"):
            print( "Bulk Bucket size set to: ", arg)
            config["bulkSize"] = int(arg)
        elif opt in ("-o"):
            print("Ordered bulk operations enabled")
            config["ord=True"]
        elif opt in ("-r"):
            print( "Padding size set to: ", arg)
            config["padSize"] = int(arg)
        elif opt in ("-D"):
            print( "Database set to: " , arg)
            config["tdb"]=str(arg)
        elif opt in ("-R"):
            print( "Replica Set name  set to: " , arg)
            config["repSet"]=str(arg)
        elif opt in ("-C"):
            print( "Collection set to: " , arg)
            config["tcoll"]=str(arg)
        elif opt in ("-U"):
            print( "Username set to: ", arg)
            config["username"] = arg
        elif opt in ("-P"):
            print("Password set to: ", arg)
            config["password"] = arg
        elif opt in ("-x"):
            print( "Port is set to: ", arg)
            config["port"] = int(arg)
        elif opt in ("-T"):
            config["docType"]=int(arg)
            print("Document Type set to: " + str(config["docType"]))
        elif opt in ("-t"):
            print( "Target is set to: ", arg)
            config["target"] = arg
        elif opt in ("-m"):
            print( "Issuing Find Commands")
            config["findOn"] = True
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
    config["message"] = ""
    for s in range(config["padSize"]):
        config["message"] += str(s)
    config["record_count"] = config["total_records"] // config["process_count"]
    testDoc = jdoc.generateDocument(config["docType"], config["message"], config["faker"])
    logging.debug("Size of record is : %f bytes" % (sys.getsizeof(testDoc)))
    # logging.debug("Size of record is : %f bytes" % (sys.getsizeof(jdoc.generateDocument(docType,message))))
    jobs = []

    for i in range(config["process_count"]):
        if config["bulk"] == False:
            if config["findOn"] == True:
                p = multiprocessing.Process(target=findWorker, args=(config,))
            else:
                p = multiprocessing.Process(target=worker, args=(config,))
                jobs.append(p)
        else:
            p = multiprocessing.Process(target=bulkworker, args=(config,))
            jobs.append(p)
        p.start()

    main_process = multiprocessing.current_process()
    logging.debug('Main process is %s %s' % (main_process.name, main_process.pid))

    for i in jobs:
        i.join()

    if config["drop"] == True:
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

def connector(config):

	if config["retries"] < config["retry_count"]:
		try:
			connection = MongoClient(config["target"],config["port"],replicaSet=config["repSet"],serverSelectionTimeoutMS=2000,connectTimeoutMS=2000)
			if config["username"] != "":
				connection.admin.authenticate(config["username"],config["password"])
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

def findWorker(config):
    p = multiprocessing.current_process()
    logging.debug("You have entered the single threaded findWorker for the process %s" % p.name)
    start_time = time.time()
    logging.info("Starting Process %s %d at %s" % (p.name, p.pid, start_time))
    logging.info("Finding %d records" % config["record_count"])
    
    connection = connector(config)
    db = connection[config["tdb"]]
    col_test = db[config["tcoll"]]
    print(col_test)
    findString = { "fld2" : "dolor sit amet, consetetur" }
    findString2 = { "fld_xx" : "Non si Trova" }
    findString3 = { "fld9" : 411900 }
    
    for i in range(config["record_count"]):
        r = col_test.find(findString)
        s = col_test.find(findString2)
        t = col_test.find(findString3)
        print( str(r.count()) + " " + str(s.count()) + " " + str(t.count()))
    
def worker(config):
	jdoc = config["jdoc"]
	p = multiprocessing.current_process()
	logging.debug("You have entered the single threaded worker for the process %s"% p.name)
	start_time = time.time()
	logging.info("Starting Process %s %d at %s" % (p.name, p.pid, start_time))
	logging.info("Inserting %d records" % config["record_count"])
	#connection = MongoClient(target,port,replicaSet=repSet)
	#if username != "":
	#    connection.admin.authenticate(username,password)
	connection = connector(config)
	db = connection[config["tdb"]]
	col_test = db[config["tcoll"]]
	for i in range(config["record_count"]):
		try:
			myInserts = col_test.insert_one(jdoc.generateDocument(config["docType"],config["message"]))
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

				myInserts = col_test.insert_one(jdoc.generateDocument(config["docType"],config["message"]))

		time.sleep(1)
	end_time = time.time()

	logging.info("Elapsed Time for job %s was %g seconds" % (p.name , end_time - start_time))

	return

def dropper(config):
    connection = MongoClient(target,port,replicaSet=repSet)
#    if username != "":
#        connection.admin.authenticate(username,password)
#    db = connection[tdb]
    connection = connector(config)
    db = connection[config["tdb"]]
    col = db[config["tcoll"]]
    logging.info("Dropping collection")
    col.drop()

def bulkworker(config):

    jdoc = config["jdoc"]
    p = multiprocessing.current_process()
    logging.debug("You have entered the bulk writer process for process %s" % p.name)
    start_time = time.time()
    logging.info("Starting Process %s %d at %s" % (p.name, p.pid, start_time))
    logging.info("Inserting %d records" % config["record_count"])
    #connection = MongoClient(target,port,replicaSet=repSet)
    #if username != "":  
    #    connection.admin.authenticate(username,password)
    connection = connector(config)
    db = connection[config["tdb"]]
    col_test = db[config["tcoll"]]
    
        
    
    request = []
    i = 0
    balance = config["record_count"]
    while (balance > 0 ):
        
        if balance > config["bulkSize"]:
            balance = balance - config["bulkSize"]
            batchSize = config["bulkSize"]
        else:
            batchSize = balance
            balance = 0
                
        for r in range(batchSize):

            request.append(InsertOne(jdoc.generateDocument(config["docType"],config["message"], config["faker"])))
            
        try : 
            bulk_result = col_test.bulk_write(request,ordered=config["ord"])
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
    multiprocessing.freeze_support()
    main()
