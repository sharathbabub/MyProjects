from __future__ import division
import numpy as np
from readData import getDataTrain
from readData import getDataTest
from collections import Counter
from similarityValue import correlationSimilarity
from similarityValue import vectorSimilary
import math

trainDict={}
meanVoteTrainDict={}

testDict={}
meanVoteTestDict={}
predictDict={}
resultDict={}
def main():
    global trainDict
    global testDict
    
    global meanVoteTrainDict
    global meanVoteTestDict
    
    global predictDict
    global resultDict
    
    dataTraining=getDataTrain("train.txt")
    dataTest=getDataTest("test5.txt")

    dataTraining = np.array(dataTraining)
    dataTest=np.array(dataTest)

    for i in range(0,len(dataTraining)):
        trainDict[i+1]=dataTraining[i]
        denominator=len(dataTraining[i])-Counter(dataTraining[i])[0]
        if denominator!=0:
            meanVoteTrainDict[i+1]=sum(dataTraining[i])/(denominator)
        else:
            meanVoteTrainDict[i+1]=0
    
    print "training data read"
    
    for i in range(201,301):
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
            testUId=testItem[0]
            testMId=testItem[1]
            testUMR=testItem[2]
            predictDict[testUId].append([testMId,testUMR])
    
    print "test data read"
    
    for i in range(201,301):
        denominator=len(testDict[i])-Counter(testDict[i])[0]
        if denominator!=0:
            meanVoteTestDict[i]=sum(testDict[i])/denominator
        else:
            meanVoteTestDict[i]=0
    print "predicting start"
    
    res= open("result5S.txt","w")
    for i in range(201,301):
        print i
        activeID=i
        activeMean=meanVoteTestDict[i]
        testcases=predictDict[i]
        for j in range(0,len(testcases)):
            movieId=testcases[j][0]
            summ=0
            normFactor=0
            for k in range(1,201):
                sim=vectorSimilary(testDict[i],trainDict[k])
                if sim!=0:
                    summ=summ+sim* (trainDict[k][movieId-1]-meanVoteTrainDict[k])
                    normFactor=normFactor+abs(sim)
            predictRating=1
            if(normFactor!=0):
                predictRating=activeMean+(summ/normFactor)
                predictRating=round(predictRating)
                if predictRating>5:
                    predictRating=5
                elif predictRating<1:
                    predictRating=1
            predictRating=int(predictRating)
            wstr=str(activeID)+" "+str(movieId)+" "+str(predictRating)+"\n"
            print wstr
            res.write(wstr)
            resultDict[activeID].append([movieId,predictRating])
    res.close()
    
    print "predicting over"
    '''
    for i in range(201,301):
        case=resultDict[i]
        for j in range(0,len(case)):
            wstr=str(i)+" "+str(case[j][0])+" "+str(case[j][1])+"\n"
            res.write(wstr)
    res.close()       
    print "finally over"
    '''
if __name__ == '__main__':
    main()
