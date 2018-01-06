
import breeze.linalg.split
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._

object PlayGround extends App {



//  @transient lazy val conf: SparkConf = new SparkConf()
//    .setMaster("local").setAppName("SVDsearchEnginee")
//  @transient lazy val sc: SparkContext = new SparkContext(conf)
//
//

  @transient val spark = SparkSession.builder
      .config("spark.sql.warehouse.dir", "file:///C:/Users/admin/IdeaProjects/search/spark-warehouse")
    .master("local")
    .appName("SVDsearchEnginee")
    .getOrCreate()
  def toBagOfWords(path: String): RDD[(String, Int)] = {
    toBagOfWords(spark.sparkContext.textFile(path))
  }

  def toBagOfWords(rdd: RDD[String]): RDD[(String, Int)] = {
    rdd
      .flatMap(_.split("\\W+"))
      .map(_.toLowerCase)
      .map { word => (word, 1) }
      .reduceByKey(_ + _)
  }

//
//
//
  val x = toBagOfWords(spark.sparkContext.textFile("C:\\Users\\admin\\IdeaProjects\\search\\arts\\*")).collect()
  x.foreach(println)

  val stemmer = new feature.Stemmer()
  val data  = spark.createDataFrame(x).toDF("word","id")
  data.printSchema()
  val toArray = udf[Array[String], String]( _.split(" "))
  val featureDf = data
    .withColumn("word", toArray(data("word")))
  featureDf.printSchema()
  val stemmed =stemmer
    .setInputCol("word")
    .setOutputCol("stemmed")
    .setLanguage("English")
    .transform(featureDf)


  stemmed.show()

}
