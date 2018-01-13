import org.apache.spark.rdd.RDD
import SparkConf._
import utils._
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
object dataframes extends App{

  def getRDD: RDD[(String,String)] = {
   if(appConf.isDataStored) sc.objectFile(appConf.rddStorage).cache()
   else sc.wholeTextFiles(appConf.textDirectory).cache()
  }

  val rdd: RDD[(String,String)] = getRDD
  val searchEngine = SearchEngine(rdd)

//  val artUrlMap =searchEngine.articleUrls.asMap
//  val bagMap = searchEngine.bagOfWords.asMap
//  val rdd =searchEngine

  println("zrobione parsowanie")


//
  val c:RDD[IndexedRow] = searchEngine.rddToIndexedRows(rdd)




  //
//  println("zrobione indeksowanie")
//  val dupa = indexRDD(rdd).collect()
  val z:RDD[(Long,SparseVector)] = transposeRows(c).mapValues(IDF)

//  val dx = z.computeSVD(300,computeU = true)
//  dx.s.toDense.toArray.foreach(println)

  val str = sc.parallelize(List("Trump Binladin saudi arabia deal with Czech and Latvia ultimatum mercury"))
  val ghj:SparseVector = searchEngine.indexEngine.indexRDD(str).collect()(0)
  print('l')




  transposeRows(toIndexedRowRDD(z)).mapValues((v:SparseVector) => corelation(v,ghj))
    .sortBy(_._2)
    .map(x => (searchEngine.artUrlMap.get(x._1),x)  )
    .collect()
    .foreach(println)

  if(!appConf.isDataStored){
  val serializer = new Serializer
  serializer.serialize(searchEngine)
  serializer.serialize(rdd)
}
//  serializer.serialize(indexEngine)

}
