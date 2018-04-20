from __future__ import print_function

import sys


from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark import SparkContext
from pyspark.mllib.classification import SVMWithSGD, SVMModel
from pyspark.mllib.regression import LabeledPoint

# Load and parse the data
def parsePoint(line):
    values = [float(x) for x in line.split(',')]
    return LabeledPoint(values[968], values[:967])

if __name__ == "__main__":
	
    sc = SparkContext(appName="PythonSVM")
    data = sc.textFile("hdfs://jefferson-city:30102/data2/combinedTrain.csv")
    parsedData = data.map(parsePoint)
    # Build the model
    #val1 = SVMWithSGD();
    #val1.optimize.setRegParam(0.5).setNumIterations(500)
    #model = val1.run(parsedData)
    model = SVMWithSGD.train(parsedData, iterations=600, regParam = 0.0001)
    # Evaluating the model on training data
    labelsAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
    trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(parsedData.count())
    print("Training Error = " + str(trainErr))
    #saving the model and printing
    #model.save(sc,'target/tmp/mySVMmodel')
    data2 = sc.textFile("hdfs://jefferson-city:30102/data2/test_combined.csv")
    testdata = data2.map(parsePoint)
    labelsAndPreds2 = testdata.map(lambda p: (p.label, model.predict(p.features)))
    testerror = labelsAndPreds2.filter(lambda (v, p): v != p).count() / float(testdata.count())
    print("Testing Error = " + str(testerror))
    metrics = MulticlassMetrics(labelsAndPreds2)
    print("the count"+ str(testdata.count()))
    class1 = labelsAndPreds2.filter(lambda (v,p): p == 1 and v == 1.0).count()
    class0 = labelsAndPreds2.filter(lambda (v,p): p == 0 and v == 0.0).count()
    print("the predicted 1 class is " + str(class1))
    print("the predicted 1 class is " + str(class0))
    #print(metrics.confusionMatrix().toArray)
    sc.stop()

