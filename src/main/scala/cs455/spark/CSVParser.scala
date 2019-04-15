package cs455.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.StringIndexer

class CSVParser(sc: SparkSession) {

  def csvToLibSVM(songPath: String, artistPath: String, libSVMPath: String, minCount : Int): Int = {
    var songInput = sc.read.format("csv").option("header", "true").load(songPath+"*.csv")
    var artistInput = sc.read.format("csv").option("header", "true").load(artistPath+"*.csv")

    val songColumns = Seq("song_id", "duration", "end_of_fade_in", "key", "loudness", "mode", "tempo", "time_signature", "start_of_fade_out")
    val artistColumns = Seq("artist_id", "artist_name", "song_id")

    songColumns.foreach(x => print(x + " "))

    sc.sqlContext.udf.register("strToNull", (s: String) => if(s.trim().isEmpty) null else s.trim())
    sc.sqlContext.udf.register("strToDouble", (s: String) => s.toDouble)
    sc.sqlContext.udf.register("strToInt", (s: String) => s.toInt)
    sc.sqlContext.udf.register("strToHash", (s: String) => s.hashCode)

    songInput = songInput.selectExpr("strToNull("+songColumns(0)+")", "strToNull("+songColumns(1)+")", "strToNull("+songColumns(2)+")",
          "strToNull("+songColumns(3)+")", "strToNull("+songColumns(4)+")", "strToNull("+songColumns(5)+")",
          "strToNull("+songColumns(6)+")", "strToNull("+songColumns(7)+")", "strToNull("+songColumns(8)+")").na.drop()
    songInput = songInput.toDF(songColumns: _*)

    artistInput = artistInput.selectExpr("strToNull(artist_id)", "strToNull(artist_name)", "strToNull(song_id)").na.drop().orderBy(asc("artist_id"))
    artistInput = artistInput.toDF(artistColumns: _*)

    val filteredArtists = artistInput.groupBy("artist_id").agg(count("artist_id")).where("count(artist_id) > " + minCount)

    val index = new StringIndexer()
      .setInputCol("artist_id")
      .setOutputCol("artist_index")

    artistInput = artistInput.join(filteredArtists, Seq("artist_id"), "inner")

    artistInput = index.fit(artistInput).transform(artistInput)
    artistInput.show(false)

    val joinedInput = songInput.join(artistInput, Seq("song_id"), "inner")
    joinedInput.show()

    val classes = joinedInput.agg(countDistinct("artist_index")).first().get(0).toString.toInt

    val file = joinedInput.rdd.map(row => {
      val builder = new StringBuilder
      builder.append(row.get(12)).append(" ")
      builder.append("1:").append(row.get(1)).append(" ")
      builder.append("2:").append(row.get(2)).append(" ")
      builder.append("3:").append(row.get(3)).append(" ")
      builder.append("4:").append(row.get(4)).append(" ")
      builder.append("5:").append(row.get(5)).append(" ")
      builder.append("6:").append(row.get(6)).append(" ")
      builder.append("7:").append(row.get(7)).append(" ")
      builder.append("8:").append(row.get(8)).append(" ")
      builder.toString()
    }).repartition(1).saveAsTextFile(libSVMPath)
    return classes
  }
}
