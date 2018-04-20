
# Multi Layer Perceptron

from __future__ import print_function
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from pyspark.sql import SparkSession

if __name__ == "__main__":
		spark = SparkSession\
        .builder.appName("multilayer_perceptron_classification_example").getOrCreate()

    # $example on$
    # Load training data
    data = spark.read.format("libsvm")\
        .load("hdfs://austin:30162/new/finalper")
    # Split the data into train and test
    splits = data.randomSplit([0.6, 0.4], 1234)
    train = splits[0]
    test = splits[1]
    # specify layers for the neural network:
    # input layer of size 4 (features), two intermediate of size 5 and 4
    # and output of size 3 (classes)
    layers = [968, 5, 4, 2]
    # create the trainer and set its parameters
    trainer = MultilayerPerceptronClassifier(maxIter=100, layers=layers, blockSize=128, seed=1234)
    # train the model
    model = trainer.fit(train)
    # compute accuracy on the test set
    result = model.transform(test)
    predictionAndLabels = result.select("prediction", "label")
    evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
    print("************************************************************")
    print("Accuracy: " + str(evaluator.evaluate(predictionAndLabels)))
    print("++++++++++************************************************************+++++++")
    # $example off$

    spark.stop()
