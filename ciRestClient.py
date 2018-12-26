import MultipartPostHandler
import urllib2
import json
import os.path
import os
from time import sleep

## this method builds the json for the submission based on the template file
## the template file (submission.json) contains the all other information you need to submit

## To find the keys below, go use a REST client to hit the endpoint http://rapid.ncsa.illinois.edu:8890/workflows/5b098a97-18b8-4932-8bc6-993e5e9f7635

# "e506cfc1-3452-4878-a3fe-05cd822127fe": "" // model start date for step 1
# "db9be147-92d0-4281-e65a-96ced12dc953": "", // model end date for step 1
# "61671392-bc2c-4286-db25-8d350ce5fd0f": "", //model start date for step 3
# "d387ef16-000b-4311-d569-6aeba171721f": "", // viz start date for step 3
# "c87494a0-5727-42b2-a6ac-1a451e33b61a": "", //viz end date for step 3


## required datasets - do not change
# b859f2b4-8a55-4f80-c1ff-8a7558df3412: "" // step 3 input 2

## These are the datasets you need to upload and provide as input
## If they don't change, save the key that you get back when you upload the data so you don't upload with each run

# 751e6af8-02bf-454f-d824-b3d522b3c4c3: ""  // k - file
# cc7746bd-4c15-41a4-aa2e-6cadd40f723c: "" // x - file
# e9f6e00f-0b5a-4e57-dc82-0167f1c7b3f1: "" // rapid connectivity file
# e79bd191-0f54-4e77-85b0-91ea97d23627: "" // basin ids
# b8af75e9-7ff7-4df0-c6ac-a6cc0b18e9be: "" // m3 - river


def buildSubmissionJson(template, k_file_dataset_id, x_file_dataset_id, connect_dataset_id, basin_dataset_id, m3_riv_dataset_id, data_start_date, data_end_date, viz_start_date, viz_end_date):
    data_start_date_key = "e506cfc1-3452-4878-a3fe-05cd822127fe"
    data_end_date_key = "db9be147-92d0-4281-e65a-96ced12dc953"
    model_start_date_key = "61671392-bc2c-4286-db25-8d350ce5fd0f"
    viz_start_date_key = "d387ef16-000b-4311-d569-6aeba171721f"
    viz_end_date_key = "c87494a0-5727-42b2-a6ac-1a451e33b61a"

    m3_river_dataset_key = "b8af75e9-7ff7-4df0-c6ac-a6cc0b18e9be";
    k_file_dataset_key = "751e6af8-02bf-454f-d824-b3d522b3c4c3";
    x_file_dataset_key = "cc7746bd-4c15-41a4-aa2e-6cadd40f723c";
    rapid_connect_dataset_key = "e9f6e00f-0b5a-4e57-dc82-0167f1c7b3f1";
    basin_id_dataset_key = "e79bd191-0f54-4e77-85b0-91ea97d23627";

    fjson = open(template, "r")
    t = fjson.readlines()

    j = json.loads("".join(t))
    j['title'] = "rapid"
    j['creatorId'] = "8bad574d-1f58-40b8-9d45-604806dcb411"
    #j['creatorId'] = "03a761d0-5f7f-452e-8395-84780bd75dde"

    j['parameters'][data_start_date_key] = data_start_date
    j['parameters'][data_end_date_key] = data_end_date
    j['parameters'][viz_start_date_key] = viz_start_date
    j['parameters'][viz_end_date_key] = viz_end_date
    j['parameters'][model_start_date_key] = data_start_date

    j['datasets'][k_file_dataset_key] = k_file_dataset_id
    j['datasets'][x_file_dataset_key] = x_file_dataset_id
    j['datasets'][rapid_connect_dataset_key] = connect_dataset_id
    j['datasets'][basin_id_dataset_key] = basin_dataset_id
    j['datasets'][m3_river_dataset_key] = m3_riv_dataset_id

    return j


def submit(restServer, submissionJson):
    ## this method submit the json to the REST endpoint
    url = restServer+"/executions"
    data = json.dumps(submissionJson)

    req = urllib2.Request(url, data, {'Content-Type': 'application/json'})
    f = urllib2.urlopen(req)
    response = f.read()
    f.close()
    return response


def getExecution(restServer, executionId):
    ## this method retrieve the execution json by id
    url = restServer+"/executions/"+executionId
    req = urllib2.Request(url)
    f = urllib2.urlopen(req)
    response = f.read()
    execution = json.loads(response)
    f.close()
    return execution


def getDataset(restServer, datasetId):
    ## this method retrieve the dataset json by id
    url = restServer+"/datasets/"+datasetId
    req = urllib2.Request(url)
    f = urllib2.urlopen(req)
    response = f.read()
    dataset = json.loads(response)
    f.close()
    return dataset


def getFileDescriptor(restServer, fdId):
    ## this method retrieve the FileDescriptor json by id
    url = restServer+"/files/"+fdId
    req = urllib2.Request(url)
    f = urllib2.urlopen(req)
    response = f.read()
    fileDescriptor = json.loads(response)
    f.close()
    return fileDescriptor


def getFile(restServer, fdId, storeDir):
    ## this method download the file with fileDescriptor id
    fd = getFileDescriptor(restServer, fdId)
    filename = fd['filename']
    url = restServer+"/files/"+fdId+"/file"
    #print "url = "+url
    u = urllib2.urlopen(url)
    localFile = open(os.path.join(storeDir, filename), 'wb')
    localFile.write(u.read())
    localFile.close()
    u.close()


def checkStatus(restServer, executionId):
    ## this method checks the status of the execution     
    execution = getExecution(restServer, executionId)
    
    return execution['stepState']


def getResults(restServer, executionId, storeDir):
    ## this method download the result zip file containing image files
    execution = getExecution(restServer, executionId)
    # result.nc : 41345eaa-e383-4715-a689-2f8b86dd4fbd
    datasetId = execution['datasets']["41345eaa-e383-4715-a689-2f8b86dd4fbd"]
    dataset = getDataset(restServer, datasetId)
    fd = dataset['fileDescriptors'][0]
    rapidFile = getFile(restServer, fd['id'], storeDir)
    return rapidFile


def uploadDataset(restServer, datasetFileName, email):
    ## upload dataset and return the dataset id
    url = restServer+"/datasets"
    params = {'useremail': email, 'uploadedFile': open(datasetFileName, 'rb')}
    opener = urllib2.build_opener(MultipartPostHandler.MultipartPostHandler)
    urllib2.install_opener(opener)
    req = urllib2.Request(url, params)
    datasetId = urllib2.urlopen(req).read().strip()
    return datasetId

def resultId(restServer, executionId, storeDir):
    ## this method download the result zip file containing image files
    execution = getExecution(restServer, executionId)
    # result.nc : 41345eaa-e383-4715-a689-2f8b86dd4fbd
    datasetId = execution['datasets']["41345eaa-e383-4715-a689-2f8b86dd4fbd"]
    return datasetId
    
def purgeDataset(restServer,datasetid):
    url = restServer + "/datasets/"+datasetid + "/purge"
    req = urllib2.Request(url)
    req.get_method = lambda: 'PUT'
    f = urllib2.urlopen(req)
    response = f.read()
    f.close()

def getDatasets(rapidServer):
    url = rapidServer+"/datasets/"
    req = urllib2.Request(url)
    f = urllib2.urlopen(req)
    response = f.read()
    dataset = json.loads(response)
    f.close()
    return dataset
##
## -----------------------------------------------------
## the following commented lines are code examples
## -----------------------------------------------------

### ## upload the m3_riv data
# datasetid0 = uploadDataset(rapidServer, "/Users/Tingting/Dropbox/Research/Data/RAPID/UpperGuad/k_UpperGuad.csv", email)
# print "uploaded dataset id:", datasetid0

# datasetid1 = uploadDataset(rapidServer, "/Users/Tingting/Dropbox/Research/Data/RAPID/UpperGuad/x_UpperGuad.csv", email)
# print "uploaded dataset id:", datasetid1
#
# datasetid2 = uploadDataset(rapidServer, "/Users/Tingting/Dropbox/Research/Data/RAPID/UpperGuad/rapid_connect_UpperGuad.csv", email)
# print "uploaded dataset id:", datasetid2
#
# datasetid3 = uploadDataset(rapidServer, "/Users/Tingting/Dropbox/Research/Data/RAPID/UpperGuad/basin_id_UpperGuad.csv", email)
# print "uploaded dataset id:", datasetid3

# datasetid0 = "f94ed29c-5e71-4c4d-923f-fa7820c4edd3"
# datasetid1 = "a2b24aa0-39e6-4227-abfc-e2c7387d9edd"
# datasetid2 = "dfbe6624-bd2b-47c3-98ea-8d963c2e69d2"
# datasetid3 = "f0f89d7c-b34f-4b2f-bf3e-3fc2260fd037"
#
# datasetid4 = uploadDataset(rapidServer, "m3_riv.nc", email)
# print "uploaded dataset id:", datasetid4
#
# #
## ## HOW TO SUBMIT the JOB
# sJson = buildSubmissionJson("submission.json", datasetid0, datasetid1, datasetid2, datasetid3, datasetid4, "2013-04-01", "2013-04-15", "2013-04-01", "2013-04-15")
# executionId = submit(rapidServer, sJson)
# print "current execution id:", executionId
# ##
### ## HOW to check the status of the workflow
# modelNotFinished = True
# ##
# jobStatus = checkStatus(rapidServer, executionId)
# print jobStatus
#
# while modelNotFinished:
#
#     sleep(10)
#
#     jobStatus = checkStatus(rapidServer, executionId)
#     print jobStatus
#
#     modelNotFinished = jobStatus.values()[0] != "FINISHED" or jobStatus.values()[1] != "FINISHED" or jobStatus.values()[2] != "FINISHED"
#
# getResults(rapidServer, executionId, os.getcwd())
# ##
### HOW to download the file
#getImgZip(rapidServer, executionId, os.getcwd())

##f = utllib.urlopen("http://rapid.ncsa.illinois.edu:8888/executions")
##s = f.read(
