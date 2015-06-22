from __future__ import division
import numpy as np
import math

def correlationSimilarity(activeVector,activeMean,dbUserVector,dbUserMean):
    numerator=0
    denominator1=0
    denominator2=0
    for i in range(0,len(activeVector)):
        if(activeVector[i]!=0 and dbUserVector[i]!=0):
            numerator=numerator+(activeVector[i]-activeMean)*(dbUserVector[i]-dbUserMean)
            denominator1=denominator1+((activeVector[i]-activeMean)*(activeVector[i]-activeMean))
            denominator2=denominator2+((dbUserVector[i]-dbUserMean)*(dbUserVector[i]-dbUserMean))
    denominator=denominator1*denominator2
    
    similarity=0
                
    if(denominator!=0):
        similarity=numerator/math.sqrt(denominator)
    else:
        similarity=0
    return similarity
    

def vectorSimilary(activeVector,dbUserVector):
    activeNormVec=activeVector*activeVector
    sumActive=sum(activeNormVec)
    activeNorm=math.sqrt(sumActive)
    
    dbUserNormVec=dbUserVector*dbUserVector
    sumdbUser=sum(dbUserNormVec)
    dbUserNorm=math.sqrt(sumdbUser)
    denominator=activeNorm*dbUserNorm
    
    similarity=0
    
    if(denominator!=0):
        for i in range(0,len(activeVector)):
            if(activeVector[i]!=0 and dbUserVector[i]!=0):
                similarity=similarity+((activeVector[i]*dbUserVector[i])/denominator)
    else:
        similarity=0
        
    return similarity
    
def simInverseFrequency(activeVector,dbUserVector,movieFreq):
    activeNormVec=activeVector*activeVector
    sumActive=sum(activeNormVec)
    activeNorm=math.sqrt(sumActive)
    
    dbUserNormVec=dbUserVector*dbUserVector
    sumdbUser=sum(dbUserNormVec)
    dbUserNorm=math.sqrt(sumdbUser)
    denominator=activeNorm*dbUserNorm
    
    similarity=0
    
    if(denominator!=0):
        for i in range(0,len(activeVector)):
            if(activeVector[i]!=0 and dbUserVector[i]!=0):
                similarity=similarity+((activeVector[i]*dbUserVector[i]*movieFreq[i+1])/denominator)
    else:
        similarity=0
        
    return similarity
   
def corInverseFrequency(activeVector,dbUserVector,movieFreq):
    sumfj=0
    sumfjvavi=0
    sumfjva=0
    sumfjvi=0
    sumfjva2=0
    sumfjvi2=0
    for i in range(0,len(activeVector)):
        if(activeVector[i]!=0 and dbUserVector[i]!=0):
            sumfj=movieFreq[i+1]
            sumfjvavi=movieFreq[i+1]*activeVector[i]*dbUserVector[i]
            sumfjva=movieFreq[i+1]*activeVector[i]
            sumfjvi=movieFreq[i+1]*dbUserVector[i]
            sumfjva2=movieFreq[i+1]*activeVector[i]*activeVector[i]
            sumfjvi2=movieFreq[i+1]*dbUserVector[i]*dbUserVector[i]
    u=(sumfj*sumfjva2-sumfjva*sumfjva)
    v=(sumfj*sumfjvi2-sumfjvi*sumfjvi)
    uv=u*v
    sim=0
    if uv!=0:
        sim=((sumfj*sumfjvavi)-(sumfjva*sumfjvi))/math.sqrt(uv)
    return sim