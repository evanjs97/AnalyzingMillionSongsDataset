package cs455.spark

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._


object Main {
  val localInput = "src/main/resources/"
  val hdfsInput = "hdfs://juneau:4921/"
  val sharedInput = "hdfs://augusta:8088/"
  def main(args: Array[String]): Unit = {
    if (args.length < 3 || args(1) != "-job") System.exit(1)

    if (args(0) == "-local") {
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
    }

    if (args(2) == "csv") {
      val artistInput = if (args(0) == "-hdfs") hdfsInput + args(3) else if(args(0) == "-local") localInput + args(3) else sharedInput + args(3)
      val songInput = if (args(0) == "-hdfs") hdfsInput + args(4) else if(args(0) == "-local") localInput + args(4) else sharedInput + args(5)
      val svmPath = if (args(0) == "-hdfs") hdfsInput + args(5) else if(args(0) == "-local") localInput + args(5) else sharedInput + args(5)
      val minCount = if(args.length > 6) args(6).toInt else 10

      val sc = if(args(0) == "-hdfs" || args(0) == "-shared") SparkSession.builder.appName("CSV to LibSVM").master("yarn").getOrCreate()
      else SparkSession.builder.appName("CSV to LibSVM").master("local").getOrCreate()

      val parser = new CSVParser(sc)
      parser.csvToLibSVM(songInput, artistInput, svmPath, minCount)
    }else if (args(2) == "predict") {
      val input = if (args(0) == "-hdfs") hdfsInput + args(3) else if(args(0) == "-local") localInput + args(3) else sharedInput + args(3)
      val output = if (args(0) == "-hdfs") hdfsInput + args(4) else if(args(0) == "-local") localInput + args(4) else sharedInput + args(4)
      val toHDFS = args(0) == "-hdfs"

      val sc = if(args(0) == "-hdfs") SparkSession.builder.appName("Predicting Stuff").master("yarn").getOrCreate()
      else SparkSession.builder.appName("Test").master("local").getOrCreate()
      val data = sc.read.format("libsvm").load(input+"/part-*")
//      data.show(false)
      val classes = data.agg(countDistinct("label")).first().get(0).toString.toInt
//      data.show(false)
      val classifier = new Classifier(data)
      val result = classifier.classify("test", output, classes, 8, toHDFS)
      sc.close()
      println("\nClasses: " + classes)
      println("\nClassification Accuracy: " + result)
    }

  }

}
