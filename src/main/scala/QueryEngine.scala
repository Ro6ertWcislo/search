import SparkConf.sc
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.rdd.RDD
import utils.{dotProduct, sparseVectorLength}

class QueryEngine(val indexEngine: IndexEngine,val IRM: IndexedRowMatrix) extends Serializable{
  def processQuery(query: String): RDD[(Long, Double)] ={
    val strRDD = sc.parallelize(List(query))
    val indexedQuery = indexEngine.indexRDD(strRDD).first()
    prepareInput(IRM)
      .mapValues(v => corelation(v,indexedQuery))
      .sortBy(_._2)
  }
  def prepareInput(indexedRowMatrix: IndexedRowMatrix): RDD[(Long,SparseVector)] =
    indexedRowMatrix
      .rows
      .map(row =>(row.index,row.vector))
      .mapValues(_.toSparse)

  def corelation(v1:SparseVector, v2:SparseVector):Double = {
    dotProduct(v1,v2)/(sparseVectorLength(v1)*sparseVectorLength(v2))
  }
}
