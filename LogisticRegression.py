

"""
Logistic regression 

"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel



def parsePoint(line):
    """
    Parse a line of text into an MLlib LabeledPoint object.
    """
    values = [float(s) for s in line.split(',')]
    return LabeledPoint(values[968], values[:967])


if __name__ == "__main__":

    sc = SparkContext(appName="PythonLR")
    points = sc.textFile("hdfs://jefferson-city:30102/data2/combinedTrain.csv").map(parsePoint)
    model = LogisticRegressionWithLBFGS.train(points)
    labelsAndPreds = points.map(lambda p: (p.label, model.predict(p.features)))
    trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(points.count())
    print("Training Error = " + str(trainErr))
    points2 = sc.textFile("hdfs://jefferson-city:30102/data2/test_combined.csv").map(parsePoint)
    labelsAndPreds2 = points2.map(lambda p: (p.label, model.predict(p.features)))
    realtrainErr = labelsAndPreds2.filter(lambda (v, p): v != p).count() / float(points2.count())
    print("REAL Testing Error = " + str(realtrainErr))
    #metrics = MulticlassMetrics(labelsAndPreds2)
    print("the count"+ str(points2.count()))
    class1 = labelsAndPreds2.filter(lambda (v,p): p == 1 and v ==1.0).count()
    class0 = labelsAndPreds2.filter(lambda (v,p): p == 0 and v == 0.0).count()
    print("the predicted 1 class is " + str(class1))
    print("the predicted 1 class is " + str(class0))

    sc.stop()
    
																	
    
    
