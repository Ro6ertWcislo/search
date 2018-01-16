import SparkConf.sc
import breeze.linalg.norm
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD

class QueryEngine(val indexEngine: IndexEngine, val IRM: RDD[(Long, Vector)]) extends Serializable {
  val lenMap: Map[Long, Double] =
    IRM.map(tup => (tup._1,Vectors.norm(tup._2, 2))).collect().toMap
  def processQuery(query: String): RDD[(Long, Double)] = {

    val strRDD = sc.parallelize(List(query))
    val indexedQuery = normalize(indexEngine.indexRDD(strRDD).first())
    val vecLen = Vectors.norm(indexedQuery,2)
    IRM
      .map{case (i,v) => (i,corelation(v, indexedQuery,lenMap(i),vecLen))}
      .sortBy(_._2, ascending = false)
  }
  def corelation(v1: Vector, v2: SparseVector,v1Len:Double,v2Len:Double): Double = {
    dotProduct(v1, v2) / (v1Len*v2Len)
  }




  def dotProduct(v1: Vector, v2: SparseVector): Double = {
    v2.indices.foldLeft(0.0) { (acc, ind) => acc + v1(ind) * v2(ind) }
  }

  def normalize(v: SparseVector): SparseVector = {
    val len = math.sqrt(v.values.map(value => value * value).sum)
    Vectors.sparse(v.size, v.indices, v.values).toSparse
  }

}
