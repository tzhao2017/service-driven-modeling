# -*- coding: utf-8 -*-
"""
Created on Sat Apr 23 22:48:40 2016

@author: Tingting
"""

import pandas as pd
import numpy as np
from scipy.io import netcdf
import random
import GA
import datetime
import ciRestClient
from time import sleep
import time
import pickle
import os


#
#=============================Set File Folder==================================
result_path = 'scenario1_real' # GA result folder
if not os.path.exists(result_path): # check whether the result folder has existed
    os.makedirs(result_path)
result_name = '/chromosomes_popSize100_stop20_mr02_r3.csv' # GA result file name
result_file = result_path+result_name


wd_path = '<water demand fie path>'
wd_name = 'waterRequest1.csv'
wd_file = wd_path+wd_name

cache_path = 'cache'
cache_name = '/chromo1.p'
read_cache = cache_path + cache_name
save_cache = result_path + cache_name

rapid_path = 'tmp_rapid'
if not os.path.exists(rapid_path): # check whether the rapid folder has existed
    os.makedirs(rapid_path)
runoff_name = '/m3_riv.nc'
runoff_file = rapid_path + runoff_name

nc_name = '/result.nc'
nc_file = rapid_path + nc_name

rapidServer = "http://rapid.ncsa.illinois.edu:8890/datawolf"
email = '<your email address>'


#==============================================================================
## define rapid-related functions

def calDiv(waterRequest,cutList, idFile):
    ### copy the water request list (pay attention to copy operator)
    waterVol = [x[:] for x in waterRequest]
    for i in range(len(waterVol)):
        group_id = float(waterVol[i][2])
        if group_id == 1:
            cutTime = cutList[0]
        elif group_id == 2:
            cutTime = cutList[1]
        elif group_id == 3:
            cutTime = cutList[2]
        elif group_id == 4:
            cutTime = cutList[3]
        elif group_id == 5:
            cutTime = cutList[4]
        elif group_id == 6:
            cutTime = cutList[5]
        elif group_id == 7:
            cutTime = cutList[6]
        useT = 24-cutTime
        divVol = float(waterVol[i][3])/24*useT
        waterVol[i].append(divVol)

    values = {}
    for key in idFile.keys():
        tempValues = []
        order = idFile[key]
        for i in range(len(waterVol)):
            if key == waterVol[i][1]:
                vol = waterVol[i][-1]/8
                tempValues.append(vol)
        values[order] = sum(tempValues)

    orderedDiv = []
    for i in values.keys():
        divTimeseries = [values[i]]*8
        orderedDiv.append(divTimeseries)
    
    return orderedDiv

def createRunoffFile(orderedRunoffVolumes,runoffPath):
    timeDimSize = len(orderedRunoffVolumes[0])
    print ("timeDimSize is", timeDimSize)
    comidDimSize = len(orderedRunoffVolumes)
    print ("comidDimSize is", comidDimSize)

    f = netcdf.netcdf_file(runoffPath,'w')
    f.history = 'data for RAPID input'
    f.createDimension('Time',timeDimSize)
    f.createDimension('COMID',comidDimSize)

    m3_riv = f.createVariable('m3_riv','f',('Time','COMID'))
    for i in range(timeDimSize):
        for j in range(comidDimSize):
            m3_riv[i][j] = orderedRunoffVolumes[j][i]

    f.close()

def runRapid(modelStartDate, modelEndDate):
    
    print "Retrieving runoffFile"
    runoffFilePath = runoff_file
    
    print "Uploading file to RAPID server ...."
    datasetid0 = "abbc884e-a839-4658-96c4-de39f8e2830e"
    datasetid1 = "eda0e6d0-fd6f-4af5-ae45-ddaf0834df94"
    datasetid2 = "82d9cd59-a3e7-4e96-a7fb-5e74f328c7c0"
    datasetid3 = "7dfca309-1166-4333-9f29-e0aec6f635fa"
    datasetid4 = ciRestClient.uploadDataset(rapidServer, runoffFilePath, email)

    print "uploaded dataset id:", datasetid4

    sJson = ciRestClient.buildSubmissionJson("submission-noqinit.json", datasetid0, datasetid1, datasetid2, datasetid3, datasetid4, modelStartDate, modelEndDate, modelStartDate, modelEndDate)
    executionId = ciRestClient.submit(rapidServer, sJson)

    print "current execution id:", executionId

    print "waiting for RAPID model to execute ..."

    modelNotFinished = True
    
    jobStatus = ciRestClient.checkStatus(rapidServer, executionId)
    print jobStatus
    
    while modelNotFinished == True:

        sleep(30)

        jobStatus = ciRestClient.checkStatus(rapidServer, executionId)
        print jobStatus

        modelNotFinished = jobStatus.values()[0] != "FINISHED" or jobStatus.values()[1] != "FINISHED" or jobStatus.values()[2] != "FINISHED"

        if modelNotFinished == False:

            break

    print "Downloading RAPID model results ..."
    outputPath = rapid_path
    ciRestClient.getResults(rapidServer, executionId, outputPath)
    
    print "Purging all datasets except input files ..."
    execution = ciRestClient.getExecution(rapidServer, executionId)
    datasets = execution['datasets'].values()
    for datasetid in datasets:
        	if(datasetid == '82d9cd59-a3e7-4e96-a7fb-5e74f328c7c0'):
        	    print "don't delete "+datasetid
        	elif(datasetid == 'eda0e6d0-fd6f-4af5-ae45-ddaf0834df94'):
        	    print "don't delete "+datasetid
        	elif(datasetid == 'abbc884e-a839-4658-96c4-de39f8e2830e'):
        	    print "don't delete"+datasetid
        	elif(datasetid == '7dfca309-1166-4333-9f29-e0aec6f635fa'):
        	    print "don't delete"+datasetid
        	elif(datasetid == '7d7dea87-7265-4545-aaef-a4ac4cf804ee'):
        	    print "don't delete"+datasetid
        	else:
        	    ciRestClient.purgeDataset(rapidServer, datasetid)    

    NCfile = nc_file
    Qout = []
    f = netcdf.netcdf_file(NCfile, 'r')
    length = len(f.variables['Qout'][:])

    for a in range(length):
        Qout.append(f.variables['Qout'][a])

    Qout = np.array(Qout)

    return Qout
  
def constraint(cutList, WaterRequestList, idFile, runoff_natural, cache, modelStartDate, modelEndDate, targetDate, ecoFlow):
    if str(cutList) in cache.keys():
        print ('the cutList value is in cache')
        values = cache[str(cutList)]
    else:          
        index = (datetime.datetime.strptime(targetDate, '%Y-%m-%d')-datetime.datetime.strptime(modelStartDate, '%Y-%m-%d')).days
        
        gauge = {'08165500':'195','08166200':'321','08167000':'559','08167500':'727','08167800':'774'}
        gaugeID = '08167500'
        gaugeOrder = gauge[gaugeID]
    
        divVolume = calDiv(WaterRequestList,cutList, idFile)
        runoff_div =np.array([x[:] for x in runoff_natural])
        runoff_div[:,(index*8):(index*8+8)] = runoff_div[:,(index*8):(index*8+8)]-np.array(divVolume)
        runoff_div=runoff_div.clip(0)
    
        createRunoffFile(list(runoff_div), runoff_file)
        Qout = runRapid(modelStartDate,modelEndDate)
    
        q = Qout[:,gaugeOrder]
        dailyStr = np.zeros(len(q)/8)
        for i in range(len(q)/8):
            dailyStr[i] = np.mean(q[(i*8):(i*8+8)])
        model = dailyStr.T
        Q = model[index]
    
        Qt = ecoFlow
        values = Q-Qt    
        #### store values in cache
        key = str(cutList)
        cache[key] = values
        time.sleep(1)
 
    return values

class OptFit(GA.GeneticFunctions):
    def __init__(self, ObjType, limit, size, prob_crossover, prob_mutation, \
        bestFitValue, bestFitChromo, length, waterRequest,ID, runoff, chromoCache, modelStartDate, \
        modelEndDate, optDate, ecoFlow, worst, mean, tmpChromo, tmp_final, stop_rule, N_iteration):
    
        self.counter = 0
        self.ObjType = ObjType
        self.limit = limit
        self.size = size

        self.prob_crossover = prob_crossover
        self.prob_mutation = prob_mutation
        
        self.fitValue = bestFitValue
        self.fitChromo = bestFitChromo
        
        self.waterRequest = waterRequest
        self.ID = ID
          
        self.runoff = runoff       
        self.length = length
        self.cache = chromoCache
        
        self.modelStartDate = modelStartDate
        self.modelEndDate = modelEndDate
        self.optDate = optDate   
        
        self.ecoFlow = ecoFlow
        
        self.mean = mean
        self.worst = worst
        
        self.tmpChromo = tmpChromo

        self.tmpFinal = tmp_final
        self.stop_rule = stop_rule
        self.N = N_iteration
        pass

    def probability_crossover(self):
        return self.prob_crossover

    def probability_mutation(self):
        return self.prob_mutation

    def initial(self):
        initial_chromo = [self.random_chromo() for j in range(self.size-1)]
        initial_chromo.append([24,24,24,24,24,24,24])
        self.tmpChromo = initial_chromo
        return initial_chromo

    def fitness(self, chromo):
        fitValues = sum(i for i in chromo)
        return fitValues
        
    def check_stop(self, fits_populations):
        self.counter += 1

        self.fitChromo.append(self.elitism(fits_populations))  # The best chromo is determined using the elitism function. The best fitness while satisfying the constraints
        self.fitValue.append(self.fitness(self.elitism(fits_populations))) # the best fitness value
        
        fits = [f for f, ch in fits_populations] # Get the fitness value for each chromosome in the population

        ## the worst and average is calculated only based on the fitness values
        worst = max(fits)
        ave = sum(fits) / len(fits)
        self.worst.append(worst)
        self.mean.append(ave)  
        
        self.tmpFinal = [x[1] for x in fits_populations] # record the population at the end of each generation

        self.stop_rule.append(self.fitness(self.fitChromo[-1]))
        
        if self.counter>=self.N:
            return all(x==self.stop_rule[-self.N:][0] for x in self.stop_rule[-self.N:])
        else:
            return self.counter>=self.limit
        
        # if self.counter == self.limit:
        #     self.final = [x[1] for x in fits_populations] # record the population of the final generation

        # return self.counter >= self.limit # when the iteration reaches to the maximum limit, stop GA.


    def parents(self, fits_populations):
        while True:
            father = self.tournament(fits_populations)
            mother = self.tournament(fits_populations)
            yield (father, mother)
            pass
        pass

    def crossover(self, parents):
        father, mother = parents
        index1 = random.randint(1, self.length-1)
        index2 = random.randint(1, self.length-1)
        if index1 > index2:
            index1, index2 = index2, index1
        child1 = father[:index1] + mother[index1:index2] + father[index2:]
        child2 = mother[:index1] + father[index1:index2] + mother[index2:]
        return (child1, child2)

    def mutation(self, chromosome):
        index = random.randint(0, 6)
        randomValues = np.arange(0,24.5,0.5)
        vary = random.sample(randomValues, 1)
        mutated = list(chromosome)
        mutated[index] = vary[0]
        return mutated

    def tournament(self, fits_populations):
        alicef, alice = self.select_random(fits_populations)
        bobf, bob = self.select_random(fits_populations) 
        if self.checkConstraint(alice) == 0 and self.checkConstraint(bob) == 0:
            return alice if alicef < bobf else bob
        elif self.checkConstraint(alice) == 0 and self.checkConstraint(bob) > 0:
            return alice
        elif self.checkConstraint(alice) > 0 and self.checkConstraint(bob) == 0:
            return bob
        else:
            return alice if self.checkConstraint(alice) < self.checkConstraint(bob) else bob        
            
    def elitism(self,fits_populations):
        ### the rule for elitism: sort the fits_populations from small to large
        ### then check which one is not violated the constraint.
        ### choose the one that did not violate the constraint as elitism
        ### otherwise choose the one that has smallest fitness value

        sorted_chromos = [x[1] for x in list(sorted(fits_populations))]
        for i in sorted_chromos:
            while self.checkConstraint(i)==0:
                return i
        return list(sorted(fits_populations))[0][1]

    def select_random(self, fits_populations):
        return fits_populations[random.randint(0, len(fits_populations)-1)]

    def random_chromo(self):
        randomValues = np.arange(0,24.5,0.5)
        sample = random.sample(randomValues, self.length)
        return sample
    
    def checkConstraint(self,chromo):
        constrValues = constraint(chromo, self.waterRequest, self.ID, self.runoff, self.cache, self.modelStartDate, self.modelEndDate, self.optDate, self.ecoFlow)
        #record[str(chromo)]=constrValues

        if constrValues >= 0: # if the constraint is satisfied, it retruns zero.
            return 0
        else: # if the constraint is violated, it returns the penalty.
            penalty = 0-constrValues 
            return penalty
        

if __name__ == "__main__":
    
    start_time = time.time()       

    ### GA parameters
    bestFitValue = [] # record the best fitness value in each generation
    bestFitChromo = [] # record the best fit chromo in each generation
    worst = [] # record the worst fitness value of all chromosomes in each generation
    mean = [] # record the mean fitness value of all chromosomes in each generation 
    tmpChromo = [] # record the initial chromosomes
    tmp_final = [] # record the population after each generation
    stop_rule = [] # the list of the best chromosome in each generation
    chromoCache = pickle.load(open(read_cache, "rb"))  # load cache library
    length=7 # the length of the chromosome
    N_iteration = 20 # if the best chromosome does not change for N_iteration, stop
    obj_type = 'min'
    population_size = 100
    iteration_limit = 50
    crossover_rate = 0.9
    mutation_rate = 0.2
    
    dataframe1 = pd.read_csv("waterrequest.csv")
    dataframe2 = pd.read_csv("runoff.csv")
    df = pd.read_csv(wd_file) 
     
    startDate = dataframe1['modelStartDate'].dropna()
    endDate = dataframe1['modelEndDate'].dropna()
    optDate = dataframe1['optDate'].dropna()
    ecoFlow = float(dataframe1['ecoFlow'].dropna())
    
    modelStartDate = str(datetime.datetime.strptime(startDate[0],'%m/%d/%Y %H:%M').date())
    modelEndDate = str(datetime.datetime.strptime(endDate[0],'%m/%d/%Y %H:%M').date())
    targetDate = str(datetime.datetime.strptime(optDate[0],'%m/%d/%Y %H:%M').date())    

    waterRequest = df.iloc[:,0:4].dropna().values.tolist()
    comid = list(dataframe1['connectCOMID'].dropna())
    ID = list(dataframe1['connectID'].dropna())
    idFile = dict(zip(comid,ID))
    runoff = dataframe2.values.tolist()
    runoff = np.array(runoff)   

    
    GA_opt = OptFit(obj_type, iteration_limit, population_size, crossover_rate, \
    	mutation_rate, bestFitValue, bestFitChromo, length, waterRequest, \
    	idFile, runoff, chromoCache, modelStartDate, modelEndDate, targetDate, ecoFlow, worst, mean, tmpChromo,\
        tmp_final, stop_rule, N_iteration)

    GA.GeneticAlgorithm(GA_opt).run()

    ## the best chromo over each generation is recorded in the GA_opt.fitChromo 
    ## the best one should be the one that is the last element in the list
    GAresult = GA_opt.fitChromo[-1]

    ## print the best chromosome, correspodning fitness value and the constraint violation value             
    print ("the best chromo is", GAresult)
    print ("the best fitness value is", GA_opt.fitness(GAresult))
    print ("the violation is", GA_opt.checkConstraint(GAresult))
    
    ## record the results of each generation
    FitValues = pd.DataFrame([GA_opt.fitness(ch) for ch in GA_opt.fitChromo])
    Chromos = pd.DataFrame(GA_opt.fitChromo)
    Worst = pd.DataFrame(GA_opt.worst)
    Mean = pd.DataFrame(GA_opt.mean)
    violate = pd.DataFrame(GA_opt.checkConstraint(i) for i in GA_opt.fitChromo)
    dataset = pd.concat([FitValues, Chromos, Worst, Mean, violate],axis =1)
    dataset.columns=["obj", "group1", "group2", "group3", "group4", "group5", "group6", "group7", "worst", "mean", "violate"]
    dataset.to_csv(result_file)
    
    saved_tmpFinal = GA_opt.tmpFinal # save the chromosome in the previous generation which can be used for initialization in the next step
    
    pickle.dump(chromoCache, open(save_cache, "wb")) # save the cache to the local result folder. If needed, mannuly update it in the cache folder
    pickle.dump(chromoCache, open(read_cache, "wb"))
    print("--- running time is %s seconds ---" % (time.time() - start_time))
    
