import SparkConf.sc
import org.apache.spark.mllib.linalg.{SparseVector, Vectors, Vector}
import org.apache.spark.rdd.RDD

class QueryEngine(val indexEngine: IndexEngine, val IRM: RDD[(Long, Vector)]) extends Serializable {
  def processQuery(query: String): RDD[(Long, Double)] = {
    val strRDD = sc.parallelize(List(query))
    val indexedQuery = normalize(indexEngine.indexRDD(strRDD).first())
    IRM
      .mapValues(v => corelation(v, indexedQuery))
      .sortBy(_._2, ascending = false)
  }


  def corelation(v1: Vector, v2: SparseVector): Double = {
    dotProduct(v1, v2)
  }

  def dotProduct(v1: Vector, v2: SparseVector): Double = {
    v2.indices.foldLeft(0.0) { (acc, ind) => acc + v1(ind) * v2(ind) }
  }

  def normalize(v: SparseVector): SparseVector = {
    val len = math.sqrt(v.values.map(value => value * value).sum)
    Vectors.sparse(v.size, v.indices, v.values).toSparse
  }

}
