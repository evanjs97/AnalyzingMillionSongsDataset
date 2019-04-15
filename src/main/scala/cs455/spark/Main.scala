package cs455.spark

import org.apache.spark.sql.SparkSession

object Main {
  val localInput = "src/main/resources/"
  val hdfsInput = "hdfs://juneau:4922/"
  def main(args: Array[String]): Unit = {
    val artistInput = if (args(0) == "-hdfs") hdfsInput + args(1) else localInput + args(1)
    val songInput = if (args(0) == "-hdfs") hdfsInput + args(2) else localInput + args(2)
    val svmPath = if(args(0) == "-hdfs") hdfsInput + "output/svms/" else localInput + "output/svms/"

    val sc = SparkSession.builder.appName("Test").master("local").getOrCreate()
    val parser = new CSVParser(sc)
    val classes = parser.parseCSV(songInput, artistInput, svmPath)
    print("Classes: " + classes)
    val data = sc.read.format("libsvm").load(svmPath+"/part-00000")
    data.show(false)
    val classifier = new Classifier(data)
    classifier.classify("test", classes, 8)
    sc.close()
    print("Classes: " + classes)
  }

}
