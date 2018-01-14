import org.apache.spark.rdd.RDD
import SparkConf._
import utils._
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.SparseVector

object dataframes extends App {
  val k =350

  val matrixEngineTuple = init
  val A = matrixEngineTuple._1
  val searchEngine = matrixEngineTuple._2
  val queryEngine = new QueryEngine(searchEngine.indexEngine, A)
  val str = queryEngine.processQuery("may war plastic")
  val res = searchEngine.matchResultsToUrls(str)
  res.foreach(println)
  println(A.rows.count())
  if (!appConf.isDataStored) {
    val serializer = new Serializer
    serializer.serialize(searchEngine)
    A.rows.saveAsObjectFile(appConf.rddStorage)
  }

  def init: (IndexedRowMatrix,SearchEngine) = {
    if(appConf.isDataStored){
      val A =new IndexedRowMatrix(sc.objectFile(appConf.rddStorage))
      val searchEngine = SearchEngine()
      return (A,searchEngine)
    }
    val rdd =sc.wholeTextFiles(appConf.textDirectory).cache()
    val searchEngine = SearchEngine(rdd)
    val indexedRows: RDD[IndexedRow] = searchEngine.indexEngine.rddToIndexedRows(rdd)
    val afterIDF: RDD[(Long, SparseVector)] = transposeRows(indexedRows).mapValues(IDF)
    (lowRankApproximation(afterIDF,k),searchEngine)
  }


}
