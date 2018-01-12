import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PlayGround extends App {



//  @transient lazy val conf: SparkConf = new SparkConf()
//    .setMaster("local").setAppName("SVDsearchEnginee")
//  @transient lazy val sc: SparkContext = new SparkContext(conf)
//
////
//
//  @transient val spark = SparkSession.builder
//      .config("spark.sql.warehouse.dir", "file:///C:/Users/admin/IdeaProjects/search/spark-warehouse")
//    .master("local")
//    .appName("SVDsearchEnginee")
//    .getOrCreate()
//  def toBagOfWords(path: String): RDD[(String, Int)] = {
//    toBagOfWords(spark.sparkContext.textFile(path))
//  }
//
//  def toBagOfWords(rdd: RDD[String]): RDD[(String, Int)] = {
//    rdd
//      .flatMap(_.split("\\W+"))
//      .map(_.toLowerCase)
//      .map { word => (word, 1) }
//      .reduceByKey(_ + _)
//  }
//
////
////
////
//  val x = toBagOfWords(spark.sparkContext.textFile("C:\\Users\\admin\\IdeaProjects\\search\\arts\\*")).collect()
//  x.foreach(println)
//
//  val stemmer = new feature.Stemmer()
//  val data  = spark.createDataFrame(x).toDF("word","id")
//  data.printSchema()
//  val toArray = udf[Array[String], String]( _.split(" "))
//  val featureDf = data
//    .withColumn("word", toArray(data("word")))
//  featureDf.printSchema()
//  val stemmed =stemmer
//    .setInputCol("word")
//    .setOutputCol("stemmed")
//    .setLanguage("English")
//    .transform(featureDf)
//
//
//  stemmed.show()

  def time[R](block: => R): (R, Long) = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    val timediff = t1 - t0
    println("Elapsed time: " + (timediff) + "ns")
    (result, timediff)
  }


  def IDF(v: SparseVector):SparseVector = {
  val idfFactor = v.size.toDouble / v.numActives.toDouble
  Vectors.dense(v.toArray.map(_*idfFactor)).toSparse
}

  def IDF2(v: SparseVector):SparseVector = {
    val idfFactor = v.size.toDouble / v.numActives.toDouble
    val newValues = v.values.map(_ * idfFactor)
    Vectors.sparse(v.size, v.indices, newValues).toSparse
  }
  time{ (1 to 10).foreach{ i=>
    val c =Vectors.sparse(200000*i,(1 to 100000*i).toArray,(1 to 100000*i).map(_.toDouble).toArray[Double]).toSparse
    print(IDF(c).values.length)
  }}
  println("duuuuuuuuuuupa")
  time{ (1 to 10).foreach{ i=>
    val c =Vectors.sparse(200000*i,(1 to 100000*i).toArray,(1 to 100000*i).map(_.toDouble).toArray[Double]).toSparse
    print(IDF2(c).values.length)
  }}
}
