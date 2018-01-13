import SparkConf.{appConf, sc}
import org.apache.spark.rdd.RDD

class ArticleUrls(val rdd:RDD[(String,String)])  extends Serializable{
  val textFilesRDD: RDD[(String, Long)] =rdd
    .map(_._1)
    .zipWithIndex()

  val urlsRDD: RDD[(String, String)] = sc.textFile(appConf.urlsDirectory)
      .map(splitPaths)

  val idUrlMap: Map[Long, String] =urlsRDD.join(textFilesRDD)
    .map(_._2)
    .map(tup => (tup._2,tup._1))
    .sortByKey()
    .collect()
    .toMap
  def asMap: Map[Long, String] = idUrlMap

  private def splitPaths(str:String): (String,String) = {
    val strArr = str.split(";")
    (strArr(0),strArr(1))
  }
}
