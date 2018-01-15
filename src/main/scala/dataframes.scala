import org.apache.spark.rdd.RDD
import SparkConf._
import utils._
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.{SparseVector, Vector}

object dataframes extends App {
  sc.setLogLevel("ERROR")
  val k = 350
  val results = 70
  val matrixEngineTuple = init
  val A = matrixEngineTuple._1
  val searchEngine = matrixEngineTuple._2
  val queryEngine = new QueryEngine(searchEngine.indexEngine, A)
  println("You can type your query now:")
  if (appConf.isDataStored)
    while (true) {
      val tmp = Console.readLine()
      time(query(tmp, results).foreach(tup => println(tup._1 + "   " + tup._2)))
    }


  if (!appConf.isDataStored) {
    val serializer = new Serializer
    serializer.serialize(searchEngine)
    A.saveAsObjectFile(appConf.rddStorage)
  }

  def init: (RDD[(Long, Vector)], SearchEngine) = {
    if (appConf.isDataStored) {
      val A: RDD[(Long, Vector)] = sc.objectFile(appConf.rddStorage).cache()
      val searchEngine = SearchEngine()
      return (A, searchEngine)
    }
    val rdd = sc.wholeTextFiles(appConf.textDirectory).cache()
    val searchEngine = SearchEngine(rdd)
    val indexedRows: RDD[IndexedRow] = searchEngine.indexEngine.rddToIndexedRows(rdd)
    val afterIDF: RDD[(Long, SparseVector)] = transposeRows(indexedRows).mapValues(IDF)
    (lowRankApproximation(afterIDF, k), searchEngine)
  }

  def query(text: String, k: Int): Array[(String, Double)] = {
    val str: RDD[(Long, Double)] = queryEngine.processQuery(text)
    val res: Array[(Option[String], (Long, Double))] = searchEngine.matchResultsToUrls(str, k)
    res.map(row => (row._1.get, row._2._2))
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    val timediff = (t1 - t0) / 1000000
    println("Elapsed time: " + timediff + "ms")
    result
  }


}
