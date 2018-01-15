import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkConf {


  @transient lazy val spark: SparkSession = SparkSession.builder
    .config("spark.sql.warehouse.dir", "file:///C:/Users/admin/IdeaProjects/search/spark-warehouse")
    .master("local[*]")
    .appName("SVDsearchEnginee")
    .getOrCreate()
  @transient lazy val sc: SparkContext = spark.sparkContext
  val appConf: AppConfig = new ConfigBuilder().autoConfigBuild()
}
