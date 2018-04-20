

"""
Random Forest Classification Example.
"""
from __future__ import print_function

import sys

from pyspark import SparkContext
# $example on$
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils
# $example off$

def parsePoint(line):
    """
    Parse a line of text into an MLlib LabeledPoint object.
    """
    values = [float(s) for s in line.split(',')]
    return LabeledPoint(values[968], values[:968])

if __name__ == "__main__":
    sc = SparkContext(appName="RandomForest")
    # $example on$
    # Load and parse the data file into an RDD of LabeledPoint.
    #points = sc.textFile("hdfs://jefferson-city:30102/data2/failureTrain.csv").map(parsePoint)
    #points.saveAsTextFile("hdfs://jefferson-city:30102/data2/savedlp")
    #pointsx = MLUtils.loadLabeledPoints(sc, "hdfs://jefferson-city:30102/data2/savedlp")
    #points2 = sc.textFile("hdfs://jefferson-city:30102/data2/successTrain.csv").map(parsePoint)
    #points2.saveAsTextFile("hdfs://jefferson-city:30102/data2/savedlp2")
    #pointsy = MLUtils.loadLabeledPoints(sc, "hdfs://jefferson-city:30102/data2/savedlp2")
    #finalpoints = pointsx.join(pointsy)
    #print(finalpoints.collect())
    data = MLUtils.loadLibSVMFile(sc, 'hdfs://jefferson-city:30102/samples/sparselibsvm')
    # Split the data into training and test sets (30% held out for testing)
    
    (trainingData, testData) = data.randomSplit([0.9, 0.1])

    # Train a RandomForest model.
    #  Empty categoricalFeaturesInfo indicates all features are continuous.
    #  Note: Use larger numTrees in practice.
    #  Setting featureSubsetStrategy="auto" lets the algorithm choose.
    model = RandomForest.trainClassifier(trainingData, numClasses=2, categoricalFeaturesInfo={},
                                         numTrees=9, featureSubsetStrategy="auto",
                                         impurity='gini', maxDepth=6, maxBins=32)

    # Evaluate model on test instances and compute test error
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
    print('Test Error = ' + str(testErr))
    print('Learned classification forest model:')
    txt = model.toDebugString()
    #print(txt)
    #st = sc.parallelize(txt)
    #print('the rdd')
    #print (st.collect())
    txt_file = open("/s/chopin/a/grad/akmittal/code/output.txt", "w")
    txt_file.write(txt)
    txt_file.close()
    #st.saveAsSequenceFile('hdfs://jefferson-city:30102/data2/treeResults2')

    # Save and load model
    #model.save(sc, "target/tmp/myRandomForestClassificationModel")
    #sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel")
    data2 = MLUtils.loadLibSVMFile(sc,'hdfs://jefferson-city:30102/data2/test_combinedLIBSVM')
    predictions2 = model.predict(data2.map(lambda x: x.features))
    labelsAndPredictions2 = data2.map(lambda lp: lp.label).zip(predictions2)
    #predictions2.saveAsTextFile('hdfs://jefferson-city:30102/results/res')
    realtestErr = labelsAndPredictions2.filter(lambda (v, p): v != p).count() / float(data2.count())
    print('REAL Test Error = ' + str(realtestErr))
    print("the count"+ str(data2.count()))
    class1 = labelsAndPredictions2.filter(lambda (v,p): p == 1 and v == 1.0).count()
    class0 = labelsAndPredictions2.filter(lambda (v,p): p == 0 and v == 0.0).count()
    print("the predicted 1 class is " + str(class1))
    print("the predicted 0 class is " + str(class0))
    # $example off$
