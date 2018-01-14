import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.rdd.RDD

object utils {
  def transposeRows(rdd:  RDD[IndexedRow]):RDD[(Long,SparseVector)] = {
    new IndexedRowMatrix(rdd).toCoordinateMatrix().transpose().toIndexedRowMatrix().rows
      .map(idxRow => (idxRow.index, idxRow.vector))
      .sortByKey()
      .mapValues(_.toSparse)
  }
  def toIndexedRowRDD(rdd: RDD[(Long,SparseVector)]): RDD[IndexedRow] =
    rdd.map{case (i:Long,v:SparseVector)=>IndexedRow(i,v)}
  def IDF(v: SparseVector):SparseVector = {
    val idfFactor = v.size.toDouble / v.numActives.toDouble
    val newValues = v.values.map(_ * idfFactor)
    Vectors.sparse(v.size, v.indices, newValues).toSparse
  }
  def dotProduct(v1:SparseVector, v2:SparseVector):Double = {
    val eq =v1.indices.toSet.intersect(v2.indices.toSet)
    eq.foldLeft(0.0) {(acc,ind) => acc+v1(ind)+v2(ind)}
  }
  def sparseVectorLength(v:Vector):Double = 1.0//math.sqrt(v.values.map(value => value*value).sum)
  Vectors.dense(Array(1.0,2,3))
  def corelation(v1:SparseVector, v2:SparseVector):Double = {
    dotProduct(v1,v2)/(sparseVectorLength(v1)*sparseVectorLength(v2))
  }
  def multiplyIndexedRowMatrixByDiagArray(IDM: IndexedRowMatrix,diagonal:Vector): IndexedRowMatrix ={
    new IndexedRowMatrix(IDM.rows.map(x => IndexedRow(x.index,Vectors
      .dense(x.vector.toArray
        .zip(diagonal.toArray)
        .map(d => d._1*d._2)))))
  }
}
