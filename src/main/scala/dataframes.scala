import org.apache.spark.rdd.RDD
import SparkConf._
import breeze.linalg.Matrix
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}

object dataframes extends App{
  val rdd: RDD[String] = spark.sparkContext.textFile("arts/*").cache()

  val x =new BagOfWords(rdd)
  val bagMap = x.asMap
  val stemmer = new MyStemmer()

  def transform(array: RDD[(String,Int)]): linalg.Vector = {
    val indicesval = array.map(record =>(bagMap.get(record._1),record._2))
      .filter(_._1.isDefined)
      .map(tup => (tup._1.get,tup._2))
      .collect()
    val tup = indicesval.unzip
    Vectors.sparse(indicesval.length,tup._1.toArray,tup._2.toArray.map(_.toDouble))
  }

  def transform(words: Map[String,Int]): Vector = {
    val indicesval = words.map(record =>(bagMap.get(record._1),record._2))
      .filter(_._1.isDefined)
      .map(tup => (tup._1.get,tup._2)).toArray
    val tup = indicesval.unzip
    Vectors.sparse(indicesval.length,tup._1.toArray,tup._2.toArray.map(_.toDouble))
  }

  def countWords(words: Array[String]): Map[String, Int] = {
    words.groupBy(identity).mapValues(_.length)
  }

  def toSparseVector(): RDD[(Vector,Long)] = {
    rdd.map(_.split("\\W+").map(_.toLowerCase))   // transform each file into array of words
      .map(arr =>stemmer.stem(arr.zipWithIndex))  // stem words in files
     .map(countWords)
     .map(transform).zipWithIndex()                   // transform arrays to sparse vectorstoSparseVector().map((x,y) => (y,x))
  }
  println("zrobione parsowanie")

  val c:RDD[IndexedRow] = toSparseVector().map {case (vector,index) => IndexedRow(index,vector) }
  println("zrobione indeksowanie")
  val z = new IndexedRowMatrix(c).toCoordinateMatrix().transpose().toRowMatrix()
  println(z.rows.count())
  println("done" + z + "\ndone")
  print("a")

}
