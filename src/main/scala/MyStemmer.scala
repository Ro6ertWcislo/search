import org.apache.spark.mllib.feature
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import SparkConf.spark

import scala.collection.mutable

class MyStemmer() extends Serializable{
  private val stemmer = new feature.Stemmer()
    .setInputCol("word")
    .setOutputCol("stemmed")
    .setLanguage("English")
  private val toArray = udf[Array[String], String]( _.split(" "))

  private def prepareInput(words: RDD[(String,Int)]): DataFrame = {
    val data = spark.createDataFrame(words)
      .toDF("word","amount")

    data.withColumn("word", toArray(data("word")))
  }
  private def prepareInput(words: Array[(String,Int)]): DataFrame = {
    val data = spark.createDataFrame(words)
      .toDF("word","index")

    data.withColumn("word", toArray(data("word")))
  }


  def stem(words: RDD[(String,Int)]): RDD[String]={
    stemmer
      .transform(prepareInput(words))
      .rdd
      .map(_.get(2).asInstanceOf[mutable.WrappedArray[String]].array.head)
 }

  def stem(words: Array[(String,Int)]): Array[String]={
    stemmer
      .transform(prepareInput(words))
      .collect()
      .map(_.get(2).asInstanceOf[mutable.WrappedArray[String]].array.head)
  }
}
