import org.apache.spark.rdd.RDD
import SparkConf._
import com.github.fommil.netlib.BLAS
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}

object dataframes extends App{
  val rdd: RDD[String] = spark.sparkContext.textFile("arts/*").cache()

  val x =new BagOfWords(rdd)
  val bagMap = x.asMap
  val stemmer = new MyStemmer()

//  def transform(array: RDD[(String,Int)]): Vector = {
//    val indicesval = array.map(record =>(bagMap.get(record._1),record._2))
//      .filter(_._1.isDefined)
//      .map(tup => (tup._1.get,tup._2))
//      .collect()
//    val tup = indicesval.unzip
//    Vectors.sparse(indicesval.length,tup._1.toArray,tup._2.toArray.map(_.toDouble))
//  }

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

  def IDF(v: SparseVector):SparseVector = {
    val idfFactor = v.size.toDouble / v.numActives.toDouble
    val newValues = v.values.map(_ * idfFactor)
    Vectors.sparse(v.size, v.indices, newValues).toSparse
  }

  val c:RDD[IndexedRow] = indexRDD(rdd).zipWithIndex().map {case (vector,index) => IndexedRow(index,vector) }.cache()
  println("zrobione indeksowanie")
  val dupa = indexRDD(rdd).collect()
  val z = new IndexedRowMatrix(c).toCoordinateMatrix().transpose().toIndexedRowMatrix().rows.collect()
//  val a = new IndexedRowMatrix(c).rows.first()
//  val d = new IndexedRowMatrix(c).toCoordinateMatrix().toIndexedRowMatrix().rows.collect()
x.bagOfWords.take(50).foreach(println)
  print("\n\n\n")
z.foreach(a=>  {print(a.index +" : "+ a.vector.toString); print("\n")})
//  val gh = z.rows.map(_.toSparse).map(IDF)
//  println("done" + gh.count() + "\ndone")
//  print("a")
//  val dx = z.computeSVD(300,computeU = true)
//  dx.s.toDense.toArray.foreach(println)

}
