import SparkConf.sc
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.rdd.RDD

class QueryEngine(val indexEngine: IndexEngine,val IRM:IndexedRowMatrix) extends Serializable{
  def processQuery(query: String): RDD[(Long, Double)] ={
    val strRDD = sc.parallelize(List(query))
    val indexedQuery = indexEngine.indexRDD(strRDD).first()
    prepareInput(IRM)
      .mapValues(v => corelation(v,indexedQuery))
      .sortBy(_._2,ascending = false)
  }
  def prepareInput(indexedRowMatrix: IndexedRowMatrix): RDD[(Long,SparseVector)] =
    indexedRowMatrix
      .rows
      .map(row =>(row.index,row.vector))
      .mapValues(_.toSparse)

  def corelation(v1:SparseVector, v2:SparseVector):Double = {
    dotProduct(v1,v2)
  }
  def dotProduct(v1: SparseVector, v2: SparseVector): Double = {
    v2.indices.foldLeft(0.0){(acc,ind) => acc + v1(ind)*v2(ind)}
  }

  def normalize(v:SparseVector): SparseVector ={
    val len =math.sqrt(v.values.map(value => value*value).sum)
    Vectors.sparse(v.size,v.indices,v.values).toSparse
  }

}
