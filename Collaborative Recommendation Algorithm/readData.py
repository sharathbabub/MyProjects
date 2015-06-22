def getDataTrain(filename):
    f = open ( filename , 'r')
    dataTraining = [ map(int,line.split('\t')) for line in f ]
    return dataTraining

def getDataTest(filename):
    f = open ( filename , 'r')
    dataTest = [ map(int,line.split(' ')) for line in f ]
    return dataTest