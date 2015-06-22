from __future__ import division
import numpy as np
from readData import getDataTrain
from readData import getDataTest
from collections import Counter
from similarityValue import correlationSimilarity
from similarityValue import vectorSimilary
import math

def main():
    
    trainDict={}
    meanVoteTrainDict={}

    testDict={}
    meanVoteTestDict={}
    predictDict={}
    resultDict={}
    dataTraining=getDataTrain("train.txt")
    dataTest=getDataTest("test20.txt")
    
    userOff=401
    dataTraining = np.array(dataTraining)
    dataTest=np.array(dataTest)
    
    testPredic=[]
    simWeights={}
    
    for i in range(0,len(dataTraining)):
        trainDict[i+1]=dataTraining[i]
        denominator=len(dataTraining[i])-Counter(dataTraining[i])[0]
        if denominator!=0:
            meanVoteTrainDict[i+1]=sum(dataTraining[i])/(denominator)
        else:
            meanVoteTrainDict[i+1]=0
    
    print "training data read"
    
    for i in range(0+userOff,100+userOff):
        movieRating=[0]*1000
        movieRating=np.array(movieRating)
        testDict[i]=movieRating
        predictDict[i]=[]
        resultDict[i]=[]
    
    for i in range(0,len(dataTest)):
        testItem=dataTest[i]
        if(testItem[2]!=0):
            testUId=testItem[0]
            testMId=testItem[1]
            testUMR=testItem[2]
            testDict[testUId][testMId-1]= testUMR
        else:
            testPredic.append(testItem)
    
    print "test data read"
    
    for i in range(0+userOff,100+userOff):
        denominator=len(testDict[i])-Counter(testDict[i])[0]
        if denominator!=0:
            meanVoteTestDict[i]=sum(testDict[i])/denominator
        else:
            meanVoteTestDict[i]=0
    print "predicting start"
    
    
    for i in range(0+userOff,100+userOff):
        simWeights[i]=[0]*200
    for i in range(0+userOff,100+userOff):
        for k in range(1,201):
            sim=correlationSimilarity(testDict[i],meanVoteTestDict[i],trainDict[k],meanVoteTrainDict[k])
            simWeights[i][k-1]=sim

    res= open("result20.txt","w")
    for i in range(0,len(testPredic)):
        case=testPredic[i]
        activeUID=case[0]
        activeMID=case[1]
        normFactor=0
        summ=0
        for k in range(1,201):
            sim= simWeights[activeUID][k-1]
            if sim!=0:
                summ= summ+sim*(trainDict[k][activeMID-1]-meanVoteTrainDict[k])
                normFactor=normFactor+abs(sim)
        predictedRating=1
        if(normFactor!=0):
            predictedRating=meanVoteTestDict[activeUID]+(summ/normFactor)
            predictedRating=round(predictedRating)
        if predictedRating>5:
            predictedRating=5
        elif predictedRating<1:
            predictedRating=1
        predictedRating=int(predictedRating)
        wstr=str(activeUID)+" "+str(activeMID)+" "+str(predictedRating)+"\n"
        print wstr
        res.write(wstr)
    res.close()
    
    print "predicting over"

if __name__ == '__main__':
    main()
