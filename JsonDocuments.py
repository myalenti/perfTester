import random
import time
import logging
import sys
import timeit
import sys
import getopt
import ast
import json
import names
import random
import numpy as np
import os
from faker import Factory
from collections import OrderedDict
from datetime import date, datetime

class JsonDocuments():
    
    def __init__(self):
        pass
    
    def generateDocument(self, docType, message):
        #self.dockType = docType
        faker = Factory.create()
        faker.seed(os.getpid())
        if docType == 1:
            randSequence = random.sample(xrange(9999),30)
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
            
            efficiency = random.triangular(.75,.99)
            months = ["01","02","03","04","05","06","07","08","09","10","11","12"]
            q1= months[0:3]
            q2= months[3:6]
            q3= months[6:9]
            q4= months[9:12]
            qA= months[0:12]
            
            record = OrderedDict()
            record['telsaVin'] = faker.md5()
            record['metrics'] = OrderedDict()
            for i in ["2024","2025"]:
                record['metrics'][i] = OrderedDict()
                for r in months:
                    randSequence = random.sample(xrange(9999),30)
                    record['metrics'][i][r] = OrderedDict()
                    record['metrics'][i][r]['milesDriven'] = randSequence[0]
                    record['metrics'][i][r]['energyUsed'] = (randSequence[0] * 0.31) / efficiency
                    record['metrics'][i][r]['tolls'] = random.randint(0, 500)
                    record['metrics'][i][r]['avgSpeed'] = random.randint(30,50)
                    record['metrics'][i][r]['topSpeed'] = random.randint(65, 135)
                    record['metrics'][i][r]['elevationChange'] = random.randint(100, 10000)
                    record['metrics'][i][r]['accelerationMiles'] = randSequence[0] * 0.3
                    record['metrics'][i][r]['decelerationMiles'] = randSequence[0] * 0.1
                    record['metrics'][i][r]['idleHours'] = random.randint(1,15)
                    record['metrics'][i][r]['chargeHours'] = (randSequence[0] * 10) / 60 / 60
                else:
                    record['metrics'][i]['summary'] = OrderedDict()   
                    record['metrics'][i]['summary']['totalMilesDriven'] = sum( self.generateList(record, i, 'milesDriven', qA) )  
                    record['metrics'][i]['summary']['totalEnergyUsed'] = sum( self.generateList(record, i, 'energyUsed', qA))
                    record['metrics'][i]['summary']['totalTolls'] = sum( self.generateList(record, i, 'tolls', qA ))
                    record['metrics'][i]['summary']['totalElevationChange'] = sum( self.generateList(record, i, 'elevationChange', qA))
                    record['metrics'][i]['summary']['totalAccelerationMiles'] = sum( self.generateList(record, i, 'accelerationMiles', qA))
                    record['metrics'][i]['summary']['totalDecelerationMiles'] = sum( self.generateList(record, i, 'decelerationMiles', qA))
                    record['metrics'][i]['summary']['totalIdleHours'] = sum( self.generateList(record, i, 'idleHours', qA))
                    record['metrics'][i]['summary']['totalChargeHours'] = sum( self.generateList(record, i, 'chargeHours', qA))
                    record['metrics'][i]['summary']['avgSpeed'] = np.array(self.generateList(record, i, 'avgSpeed', qA)).mean()
                    record['metrics'][i]['summary']['topSpeed'] = np.amax(self.generateList(record, i, 'topSpeed', qA))
                    
                    for a,b in [ ("Q1", q1), ("Q2",q2), ("Q3", q3), ("Q4", q4)]:
                        record['metrics'][i][a] = OrderedDict()
                        record['metrics'][i][a]['totalMilesDriven'] = sum( self.generateList(record, i, 'milesDriven', b) )  
                        record['metrics'][i][a]['totalEnergyUsed'] = sum( self.generateList(record, i, 'energyUsed', b))
                        record['metrics'][i][a]['totalTolls'] = sum( self.generateList(record, i, 'tolls', b ))
                        record['metrics'][i][a]['totalElevationChange'] = sum( self.generateList(record, i, 'elevationChange', b))
                        record['metrics'][i][a]['totalAccelerationMiles'] = sum( self.generateList(record, i, 'accelerationMiles', b))
                        record['metrics'][i][a]['totalDecelerationMiles'] = sum( self.generateList(record, i, 'decelerationMiles', b))
                        record['metrics'][i][a]['totalIdleHours'] = sum( self.generateList(record, i, 'idleHours', b))
                        record['metrics'][i][a]['totalChargeHours'] = sum( self.generateList(record, i, 'chargeHours', b))
                        record['metrics'][i][a]['avgSpeed'] = np.array(self.generateList(record, i, 'avgSpeed', b)).mean()
                        record['metrics'][i][a]['topSpeed'] = np.amax(self.generateList(record, i, 'topSpeed', b))
                    
            record['drivers'] = []
            record['employee'] = OrderedDict()
            record['employee']['employeeId'] = "employeeId"
            record['employee']['saTeam'] = (random.randint(1, 3) * 1000) + random.randint(1, 500)
            #record['employee']['saRegionNum'] = random.randint(1, 3) * 1000
            
            return record
        if docType == 4:
            
            efficiency = random.triangular(.75,.99)
            months = [1,2,3,4,5,6,7,8,9,10,11,12]
            q1= months[0:3]
            q2= months[3:6]
            q3= months[6:9]
            q4= months[9:12]
            qA= months[0:12]
            
            record = OrderedDict()
            #record['vin'] = faker.md5()
            record['_id'] = faker.uuid4()
            #record['_id'] = faker.md5()
            
            record['metrics'] = []
            record['summary'] = []
            for i in [2024, 2025]:
                
                for r in months:
                    randSequence = random.sample(xrange(9999),30)
                    #record['metrics'][i][r] = OrderedDict()
                    uploadDate = datetime(i,r,28)
                    record['metrics'].append( OrderedDict() )
                    #record['metrics'].append( { 'uploadDate' : uploadDate } )
                    latest = record['metrics'][(len (record['metrics'])-1)]
                    latest['uploadDate'] = uploadDate
                    latest['milesDriven'] = randSequence[0]
                    latest['energyUsed'] = (randSequence[0] * 0.31) / efficiency
                    latest['tolls'] = random.randint(0, 500)
                    latest['avgSpeed'] = random.randint(30,50)
                    latest['topSpeed'] = random.randint(65, 135)
                    latest['elevationChange'] = random.randint(100, 10000)
                    latest['accelerationMiles'] = randSequence[0] * 0.3
                    latest['decelerationMiles'] = randSequence[0] * 0.1
                    latest['idleHours'] = random.randint(1,15)
                    latest['chargeHours'] = (randSequence[0] * 10) / 60 / 60
                
                else:
                    record['summary'].append( OrderedDict())
                    slatest = record['summary'][(len (record['summary'])-1)]
                    slatest['startDate'] = datetime((i-1),12,31)
                    slatest['endDate'] = datetime(i,12,31)
                    slatest['totalMilesDriven'] = sum( self.generateList(record, datetime(i,12,31), datetime( (i-1), 12,31) , 'milesDriven') )
                    h_milesDriven = slatest['totalMilesDriven']
                    slatest['totalEnergyUsed'] = sum( self.generateList(record, datetime(i,12,31), datetime( (i-1), 12,31) ,'energyUsed'))
                    slatest['totalTolls'] = sum( self.generateList(record, datetime(i,12,31), datetime( (i-1), 12,31) , 'tolls'))
                    slatest['totalElevationChange'] = sum( self.generateList(record, datetime(i,12,31), datetime( (i-1), 12,31) ,'elevationChange', ))
                    slatest['totalAccelerationMiles'] = sum( self.generateList(record, datetime(i,12,31), datetime( (i-1), 12,31) , 'accelerationMiles', ))
                    slatest['totalDecelerationMiles'] = sum( self.generateList(record, datetime(i,12,31), datetime( (i-1), 12,31) , 'decelerationMiles', ))
                    slatest['totalIdleHours'] = sum( self.generateList(record, datetime(i,12,31), datetime( (i-1), 12,31) , 'idleHours', ))
                    slatest['totalChargeHours'] = sum( self.generateList(record, datetime(i,12,31), datetime( (i-1), 12,31) , 'chargeHours', ))
                    slatest['avgSpeed'] = np.array(self.generateList(record, datetime(i,12,31), datetime( (i-1), 12,31) , 'avgSpeed', )).mean()
                    h_avgSpeed = slatest['avgSpeed']
                    slatest['topSpeed'] = np.amax(self.generateList(record, datetime(i,12,31), datetime( (i-1), 12,31) , 'topSpeed', ))
                    
                for a,b in [ (datetime(i,1,1), datetime(i,4,1)), (datetime(i,4,1), datetime(i,7,1)), (datetime(i,7,1), datetime(i,10,1)),(datetime(i,10,1), datetime((i+1),1,1))]:
                    record['summary'].append( OrderedDict())
                    slatest = record['summary'][(len (record['summary'])-1)]
                    slatest['startDate'] = a
                    slatest['endDate'] = b
                    slatest['totalMilesDriven'] = sum( self.generateList(record, b, a, 'milesDriven') )
                    slatest['totalEnergyUsed'] = sum( self.generateList(record, b, a, 'energyUsed'))
                    slatest['totalTolls'] = sum( self.generateList(record, b, a, 'tolls'))
                    slatest['totalElevationChange'] = sum( self.generateList(record, b, a ,'elevationChange', ))
                    slatest['totalAccelerationMiles'] = sum( self.generateList(record, b, a  , 'accelerationMiles', ))
                    slatest['totalDecelerationMiles'] = sum( self.generateList(record, b, a  , 'decelerationMiles', ))
                    slatest['totalIdleHours'] = sum( self.generateList(record, b,a , 'idleHours', ))
                    slatest['totalChargeHours'] = sum( self.generateList(record, b,a  , 'chargeHours', ))
                    slatest['avgSpeed'] = np.array(self.generateList(record, b,a  , 'avgSpeed', )).mean()
                    slatest['topSpeed'] = np.amax(self.generateList(record, b,a  , 'topSpeed', ))
                    
            record['employee'] = OrderedDict()
            for i in range(0,1):
                #record['employee'].append( OrderedDict())
                #dlatest = record['employee'][(len (record['employee'])-1 )]
                team = random.randint(1, 165)
                region = random.randint(1, 3) * 1000
                emp_id = random.randint(1,2500000)
                record['employee']['Team'] = team
                record['employee']['Region'] = region
                record['employee']['employeeId'] = (emp_id + region + team)
                
                
            else:
                record['drivers'] = []
                record['drivers'].append( OrderedDict())
                dlatest = record['drivers'][(len (record['drivers'])-1 )]
                team = random.randint(1, 165)
                region = random.randint(1, 3) * 1000
                emp_id = random.randint(1,2500000)
                firstname = names.get_first_name()
                lastname = names.get_last_name()
                dlatest['Team'] = team
                dlatest['Region'] = region
                dlatest['employeeId'] = (emp_id + region + team)
                dlatest['firstName'] = firstname
                dlatest['lastName'] = lastname
                dlatest['milesDriven'] = h_milesDriven - (h_milesDriven * efficiency) 
                dlatest['hoursDriven'] = h_milesDriven  / h_avgSpeed
            return record
        if docType == 5:
            
            efficiency = random.triangular(.75,.99)
            months = [1,2,3,4,5,6,7,8,9,10,11,12]
            q1= months[0:3]
            q2= months[3:6]
            q3= months[6:9]
            q4= months[9:12]
            qA= months[0:12]
            
            record = OrderedDict()
            #record['vin'] = faker.md5()
            record['_id'] = faker.uuid4()
            #record['_id'] = faker.md5()
            record['year'] = 2024
            record['metrics'] = []
            
            for i in months:
                
                randSequence = random.sample(xrange(9999),30)
                #record['metrics'][i][r] = OrderedDict()
                #uploadDate = datetime(i,r,28)
                
                record['metrics'].append( OrderedDict() )
                #record['metrics'].append( { 'uploadDate' : uploadDate } )
                latest = record['metrics'][(len (record['metrics'])-1)]
                latest['month'] = i
                latest['milesDriven'] = randSequence[0]
                latest['energyUsed'] = (randSequence[0] * 0.31) / efficiency
                latest['tolls'] = random.randint(0, 500)
                latest['avgSpeed'] = random.randint(30,50)
                latest['topSpeed'] = random.randint(65, 135)
                latest['elevationChange'] = random.randint(100, 10000)
                latest['accelerationMiles'] = randSequence[0] * 0.3
                latest['decelerationMiles'] = randSequence[0] * 0.1
                latest['idleHours'] = random.randint(1,15)
                latest['chargeHours'] = (randSequence[0] * 10) / 60 / 60
                
                    
            record['employee'] = OrderedDict()
            for i in range(0,1):
                #record['employee'].append( OrderedDict())
                #dlatest = record['employee'][(len (record['employee'])-1 )]
                team = random.randint(1, 165)
                region = random.randint(1, 3) * 1000
                emp_id = random.randint(1,2500000)
                record['employee']['Team'] = team
                record['employee']['Region'] = region
                record['employee']['employeeId'] = (emp_id + region + team)
                
                
            return record
        
            
        
    def getRandMonth(self):
        return random.choice([1,2,3,4,5,6,7,8,9,10,11,12])
    
    def getRandYear(self):
        return random.choice([2024,2025])
    
    def generateList(self, record, topDate, bottomDate, stat):
        statsList = []
        for i in record['metrics']:
            if bottomDate < i['uploadDate'] < topDate:
                statsList.append(i[stat])
        return statsList
        
    
