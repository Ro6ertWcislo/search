import org.apache.spark.mllib.feature
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf

import scala.collection.mutable

class MyStemmer(val spark:SparkSession) {
  private val stemmer = new feature.Stemmer()
  private val toArray = udf[Array[String], String]( _.split(" "))

  private def prepareInput(words: RDD[(String,Int)]): DataFrame = {
    val data = spark.createDataFrame(words)
      .toDF("word","amount")

    data.withColumn("word", toArray(data("word")))
  }

  def stem(words: RDD[(String,Int)]): RDD[String]={
    stemmer
      .setInputCol("word")
      .setOutputCol("stemmed")
      .setLanguage("English")
      .transform(prepareInput(words))
      .rdd
      .map(_.get(2).asInstanceOf[mutable.WrappedArray[String]].array.head)
 }
}
