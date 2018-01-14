import org.apache.spark.rdd.RDD
import SparkConf._
import utils._
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
object dataframes extends App{

//  def getIndexedRows: RDD[(String,String)] = {
//   if(appConf.isDataStored) sc.objectFile(appConf.rddStorage).cache()
//   else {sc.wholeTextFiles(appConf.textDirectory).cache()}
//  }
//
//  val rdd: RDD[(String,String)] = getIndexedRows
  val searchEngine = SearchEngine()

//  val artUrlMap =searchEngine.articleUrls.asMap
//  val bagMap = searchEngine.bagOfWords.asMap
//  val rdd =searchEngine

  println("zrobione parsowanie")


//
  //val indexedRows:RDD[IndexedRow] = searchEngine.indexEngine.rddToIndexedRows(rdd)




  //
//  println("zrobione indeksowanie")
//  val dupa = indexRDD(rdd).collect()
  val z:RDD[(Long,SparseVector)] =sc.objectFile(appConf.rddStorage).cache() //transposeRows(indexedRows).mapValues(IDF)

  val im = new IndexedRowMatrix(toIndexedRowRDD(transposeRows(toIndexedRowRDD(z))))
  val svd = im.computeSVD(100,computeU = true)
  val ss = svd.U.rows.collect()
  val d = multiplyIndexedRowMatrixByDiagArray(svd.U,svd.s).multiply(svd.V.transpose)

  val str = sc.parallelize(List("arab taxes"))
  val ghj:SparseVector = searchEngine.indexEngine.indexRDD(str).collect()(0)
  print('l')




  d.rows.map(sss =>(sss.index,sss.vector)).mapValues(_.toSparse).mapValues((v:SparseVector) => corelation(v,ghj))
    .sortBy(_._2)
    .map(x => (searchEngine.artUrlMap.get(x._1),x)  )
    .collect()
    .foreach(println)

  if(!appConf.isDataStored){
  val serializer = new Serializer
  serializer.serialize(searchEngine)
  serializer.serialize(z)
}
//  serializer.serialize(indexEngine)

}
