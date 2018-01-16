import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors, DenseVector}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.rdd.RDD

object utils {
  def transposeRows(rdd: RDD[IndexedRow]): RDD[(Long, SparseVector)] = {
    new IndexedRowMatrix(rdd).toCoordinateMatrix().transpose().toIndexedRowMatrix().rows
      .map(idxRow => (idxRow.index, idxRow.vector))
      .sortByKey()
      .mapValues(_.toSparse)
  }

  def toIndexedRowRDD(rdd: RDD[(Long, SparseVector)]): RDD[IndexedRow] =
    rdd.map { case (i: Long, v: SparseVector) => IndexedRow(i, v) }

  def IDF(v: SparseVector): SparseVector = {
    val idfFactor = math.log(v.size.toDouble / v.numActives.toDouble)
    val newValues = v.values.map(_ * idfFactor)
    Vectors.sparse(v.size, v.indices, newValues).toSparse
  }


  def dotProduct(v1: SparseVector, v2: SparseVector): Double = {
    v2.indices.foldLeft(0.0) { (acc, ind) => acc + v1(ind) * v2(ind) }
  }


  def normalize(v: Vector): Vector = {
    val norm = Vectors.norm(v, 2)
    v match {
      case v1: SparseVector => Vectors.sparse(v1.size, v1.indices, v1.values.map(_ / norm))
      case v2: DenseVector => Vectors.dense(v2.values.map(_ / norm))
    }
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    val timediff = t1 - t0
    println("Elapsed time: " + timediff + "ns")
    result
  }

  def multiplyIndexedRowMatrixByDiagMatrix(IDM: IndexedRowMatrix, diagonal: Vector): IndexedRowMatrix = {
    new IndexedRowMatrix(IDM.rows.map(x => IndexedRow(x.index, Vectors
      .dense(x.vector.toArray
        .zip(diagonal.toArray)
        .map(d => d._1 * d._2)))))
  }

  def lowRankApproximation(rdd: RDD[(Long, SparseVector)], k: Int): RDD[(Long, Vector)] = {
    val svd = new IndexedRowMatrix(
      toIndexedRowRDD(
        transposeRows(
          toIndexedRowRDD(rdd)))).computeSVD(k, computeU = true)
    val U = svd.U
    val s = svd.s
    val VT = svd.V.transpose
    multiplyIndexedRowMatrixByDiagMatrix(U, s).multiply(VT)
      .rows
      .map(row => (row.index, row.vector))
  }
}
