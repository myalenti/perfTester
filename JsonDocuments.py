import random
import time
import logging
import sys
import timeit
import sys
import getopt
import ast
import json
import datetime
import names
import random
import numpy as np
import os
from faker import Factory
from collections import OrderedDict
from datetime import date, datetime, timedelta


class JsonDocuments():
    counter1 = 0
    counter2 = 0
    def __init__(self):
        
        pass
    
    def generateDocument(self, docType, message, faker):
        #self.dockType = docType
        #faker = Factory.create()
        faker.seed(os.getpid())
        if docType == 1:
            randSequence = random.sample(range(9999),30)
            fakeText = faker.text()
            firstname = names.get_first_name()
            lastname = names.get_last_name()
            email = firstname + "." + lastname + "@mongodb.com"
            record = OrderedDict()
            record['name']  = OrderedDict()
            record['name']['firstName'] = firstname
            record['name']['lastName'] = lastname
            record['contact'] = OrderedDict()
            record['contact']['email'] = email
            record['contact']['homeAddress'] = faker.address()
            record['contact']['officeAddress'] = faker.address()
            record['age'] = random.randint(20, 72)
            record['commuteDistance'] = random.randint(2, 200)
            record['familySize'] = random.randint(2, 10)
            #record['saTeamNum'] = random.randint(1, 500)
            #record['telsaVin'] = faker.md5()
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

        if docType == 3:
            my_int=random.randint(1,13000)
            my_price=random.randint(1,100)
            record = OrderedDict()
            record['site']  = OrderedDict()
            record['site']['id'] = my_int
            record['site']['Name'] = faker.catch_phrase()
            record['item'] = OrderedDict()
            record['item']['id'] = my_int * 3
            record['item']['description'] = faker.catch_phrase()
            record['prices'] = OrderedDict()
            record['prices']['Breakfast'] = my_price/4
            record['prices']['Lunch'] = my_price/2
            record['prices']['Dinner'] = my_price
            return record
        if docType == 4:
            record = OrderedDict()
            record['a'] = random.randint(1,100)
            record['b'] = random.randint(1,300)
            record['c'] = random.randint(1,50)
            return record
        if docType == 5:
            fakeMessage = faker.text(15)
            randoUni = random.uniform(0,30)
            record = OrderedDict()
            record["meid"] = hex(random.randrange(11111111111111,99999999999999))
            record["MDN"] = hex(random.randrange(11111111111111,99999999999999))
            record["Category"] = hex(random.randrange(11111111111111,99999999999999))
            record["ESN"] = hex(random.randrange(11111111111111,99999999999999))
            record["OtherId"] = hex(random.randrange(11111111111111,99999999999999))
            for i in range(1,15):
                record[str(i) + "_ipaddress"] = faker.ipv4_private()
            for i in range(1,100):
                record[str(i) + "_metric" ] = randoUni
            for i in range(1,85):
                record[str(i) + "_words"] = fakeMessage
            for i in range(1,2000):
                record[str(i) + "_extended"] = fakeMessage
            return record
        if docType == 6:
            fakeMessage = faker.text(15)
            randoUni = random.uniform(0,30)
            record = OrderedDict()
            record["meid"] = random.randrange(11111111111111,99999999999999)
            record["MDN"] = random.randrange(11111111111111,99999999999999)
            record["Category"] = hex(random.randrange(11111111111111,99999999999999))
            record["ESN"] = hex(random.randrange(11111111111111,99999999999999))
            record["OtherId"] = hex(random.randrange(11111111111111,99999999999999))
            for i in range(1,3):
                record[str(i) + "_ipaddress"] = faker.ipv4_private()
            for i in range(1,10):
                record[str(i) + "_metric" ] = randoUni
            for i in range(1,8):
                record[str(i) + "_words"] = fakeMessage
            for i in range(1,5):
                record[str(i) + "_extended"] = fakeMessage
            return record
        if docType == 7:
            record = OrderedDict()
            record["recipient"] =  OrderedDict()
            record["recipient"]["Name"] = faker.name()
            record["recipient"]["address"] = faker.street_name()
            record["recipient"]["City"] = faker.city()
            record["recipient"]["Zip"] = faker.postalcode()
            record["recipient"]["country"] = faker.country()
            record["recipient"]["Phone Number"] = faker.msisdn()
            record["shipment"] = OrderedDict()
            record["shipment"]["weight"] = random.randrange(1,5)
            record["shipment"]["value"] = random.randrange(1,1000)
            record["shipment"]["insurance"] = OrderedDict()
            record["shipment"]["insurance"]["insured"] = True
            record["shipment"]["insurance"]["value"] = random.randint(1, 100)
            record["shipment"]["insurance"]["amount"] = random.randint(1,100)
            record["shipment"]["ShipmentDate"] = datetime.today()
            record["shipment"]["DeliveryDate"] = datetime.today()
            record["shipment"]["truckId"] = random.randint(1000,50000)
            record["shipment"]["Driver"] = random.randint(10000,150000)
            record["shipment"]["trackingNo"] = faker.password(length=25, special_chars=False, digits=True, upper_case=True, lower_case=False)
            record["adressee"] = OrderedDict()
            record["adressee"]["address"] = faker.street_name()
            record["adressee"]["City"] = faker.city()
            record["adressee"]["Zip"] = faker.postalcode()
            record["adressee"]["country"] = faker.country()
            record["adressee"]["Phone Number"] = faker.msisdn()
            record["billing"] = OrderedDict()
            record["billing"]["acctNo"] = faker.password(length=16, special_chars=False, digits=True, upper_case=False, lower_case=False)
            record["billing"]["acctType"] = random.choice( ["personal", "business", "cod"])
            record["billing"]["name"] = faker.name()
            record["billing"]["address"] = faker.address()
            record["billing"]["invoicedAmount"] = random.randrange(1, 1000)
            return record




