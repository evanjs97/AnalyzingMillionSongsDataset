package cs455.spark

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

class Classifier(data: DataFrame) {

  def classify(alg : String, output : String, classes : Int, features : Int): Unit = {
    classify(alg, output, .6, .4, classes, features)
  }

  def classify(alg : String, output : String, trainSplit : Double, testSplit : Double, classes : Int, features : Int): Unit = {
    val splits = data.randomSplit(Array(trainSplit,testSplit))
    val train = splits(0)
    val test = splits(1)
    test.show(false)
    train.show(false)
    val layers = Array(features, classes + 3, classes + 2, classes)
    multilayer(layers, train, test, output)
  }

  def multilayer(layers : Array[Int], train : Dataset[Row], test : Dataset[Row], output : String): Unit = {
    val modelTrainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(100)

    val model = modelTrainer.fit(train)

    val result = model.transform(test)

    val prediction = result.select("prediction", "label")
    val evaluate = new MulticlassClassificationEvaluator()
        .setMetricName("accuracy")
    evaluate.save(output)

    print("Multilayer Perceptron Classifier achieved an accuracy of: " + evaluate.evaluate(prediction))
  }

}
