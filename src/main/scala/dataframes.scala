import org.apache.spark.rdd.RDD
import SparkConf._
import utils._
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}

object dataframes extends App{
  val rdd: RDD[String] = spark.sparkContext.textFile("arts/*").cache()
  val x =new BagOfWords(rdd)
  val bagMap = x.asMap
  val stemmer = new MyStemmer()


  def mapToIndex(words: Map[String,Int]): SparseVector = {
    val indicesval = words.map(record =>(bagMap.get(record._1),record._2))
      .filter(_._1.isDefined)
      .map(tup => (tup._1.get,tup._2)).toArray
    val tup = indicesval.unzip
    Vectors.sparse(bagMap.size,tup._1.toArray,tup._2.toArray.map(_.toDouble)).toSparse
  }

  def countWords(words: Array[String]): Map[String, Int] = {
    words.groupBy(identity).mapValues(_.length)
  }

  def indexRDD(rdd : RDD[String]): RDD[SparseVector] = {
    rdd.map(_.split("\\W+").map(_.toLowerCase))   // transform each file into array of words
      .map(arr =>stemmer.stem(arr.zipWithIndex))  // stem words in files
     .map(countWords)                             // transforms words into Map of word -> occurences
     .map(mapToIndex)                             // transform arrays to sparse
  }
  println("zrobione parsowanie")


//
  val c:RDD[IndexedRow] = indexRDD(rdd).zipWithIndex().map {case (vector,index) => IndexedRow(index,vector) }




  //
//  println("zrobione indeksowanie")
//  val dupa = indexRDD(rdd).collect()
  val z:RDD[(Long,SparseVector)] = transposeRows(c).mapValues(IDF)

//  val dx = z.computeSVD(300,computeU = true)
//  dx.s.toDense.toArray.foreach(println)

  val str = sc.parallelize(List(" RObert forced to dupa left-singular vectors and right-singular vectors"))
  val ghj:SparseVector = indexRDD(str).collect()(0)
  print('l')




  transposeRows(toIndexedRowRDD(z)).mapValues((v:SparseVector) => corelation(v,ghj))
    .sortBy(_._2)
    .collect()
    .foreach(println)

}
