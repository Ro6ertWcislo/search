import org.apache.spark.rdd.RDD
import SparkConf._
import dataframes.time
import utils._
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.SparseVector

object dataframes extends App {
  val k =350

  val matrixEngineTuple = init
  val A = matrixEngineTuple._1
  val searchEngine = matrixEngineTuple._2
  val queryEngine = new QueryEngine(searchEngine.indexEngine, A)
  println(query("may trump be with you", 25))
  println(query("merkel wrong pakistan", 25))

  for (i <- 1 to 10){
    val tmp = Console.readLine()
    time(query(tmp,10).foreach(println))
  }

 // query("schwarzman bayn playing",25).foreach(println)

  if (!appConf.isDataStored) {
    val serializer = new Serializer
    serializer.serialize(searchEngine)
    A.rows.saveAsObjectFile(appConf.rddStorage)
  }

  def init: (IndexedRowMatrix,SearchEngine) = {
    if(appConf.isDataStored){
      val rdd:RDD[IndexedRow] =sc.objectFile(appConf.rddStorage).cache()
      val A =new IndexedRowMatrix(rdd)
      val searchEngine = SearchEngine()
      return (A,searchEngine)
    }
    val rdd =sc.wholeTextFiles(appConf.textDirectory).cache()
    val searchEngine = SearchEngine(rdd)
    val indexedRows: RDD[IndexedRow] = searchEngine.indexEngine.rddToIndexedRows(rdd)
    val afterIDF: RDD[(Long, SparseVector)] = transposeRows(indexedRows).mapValues(IDF)
    (lowRankApproximation(afterIDF,k),searchEngine)
  }
  def query(text:String,k:Int): Array[String] ={
    val str:RDD[(Long, Double)] =  time(queryEngine.processQuery(text))
    val res:Array[(Option[String], (Long, Double))] =  time(searchEngine.matchResultsToUrls(str))
      time(res.map(_._1.get).drop(res.length-k))
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    val timediff = (t1 - t0)/1000000
    println("Elapsed time: " + timediff + "ms")
    result
  }








}
