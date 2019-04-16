package cs455.spark

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

class Classifier(data: DataFrame) {

  def classify(alg : String, output : String, classes : Int, features : Int, toHDFS : Boolean): Unit = {
    classify(alg, output, .6, .4, classes, features, toHDFS)
  }

  def classify(alg : String, output : String, trainSplit : Double, testSplit : Double, classes : Int, features : Int, toHDFS : Boolean): Unit = {
    val splits = data.randomSplit(Array(trainSplit,testSplit))
    val train = splits(0)
    val test = splits(1)
    test.show(false)
    train.show(false)
    val layers = Array(features, classes + 3, classes + 2, classes)
    val results = Array(multilayer(layers, train, test, output))
    val resultTypes = Array("Multilayer Perceptron Classifier achieved an accuracy of: ")

    if(toHDFS) write(output, results, resultTypes)
  }

  def multilayer(layers : Array[Int], train : Dataset[Row], test : Dataset[Row], output : String): Double = {
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
    evaluate.evaluate(prediction)

    return evaluate.evaluate(prediction)
  }

  def write(output : String, results : Array[Double], types : Array[String]): Unit = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://juneau:4922")
    val fs = FileSystem.get(conf)
    val outStream = fs.create(new Path(output))
    val streamWriter = new PrintWriter(outStream)
    for(i <- types.indices)
    {
      streamWriter.write(types(i) + results(i))
    }
  }

}
