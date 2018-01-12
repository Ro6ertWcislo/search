import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkConf {
//  val bagOfWordsCollectionRead = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/test.bagOfWords"))
//  val bagOfWordsCollectionWrite = WriteConfig(Map("uri" -> "mongodb://127.0.0.1/test.bagOfWords"))
//  val stopWordsCollectionRead = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/test.stopWords"))
//  val stopWordsCollectionWrite = WriteConfig(Map("uri" -> "mongodb://127.0.0.1/test.stopWords"))

  @transient lazy val spark:SparkSession = SparkSession.builder
    .config("spark.sql.warehouse.dir", "file:///C:/Users/admin/IdeaProjects/search/spark-warehouse")
    .master("local[*]")
    .appName("SVDsearchEnginee")
    .getOrCreate()
  @transient lazy val sc: SparkContext = spark.sparkContext
}
