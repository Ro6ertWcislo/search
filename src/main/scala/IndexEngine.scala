import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
import org.apache.spark.rdd
import org.apache.spark.rdd.RDD

class IndexEngine(bagOfWords: BagOfWords) extends Serializable {
  private val bagMap = bagOfWords.asMap
  private val stemmer = bagOfWords.getStemmer

  private def mapToIndex(words: Map[String, Int]): SparseVector = {
    val indicesval = words.map(record => (bagMap.get(record._1), record._2))
      .filter(_._1.isDefined)
      .map(tup => (tup._1.get, tup._2)).toArray
    val tup = indicesval.unzip
    Vectors.sparse(bagMap.size, tup._1.toArray, tup._2.toArray.map(_.toDouble)).toSparse
  }

  private def countWords(words: Array[String]): Map[String, Int] = {
    words.groupBy(identity).mapValues(_.length)
  }

  def indexRDD(rdd: RDD[String]): RDD[SparseVector] = {
    rdd.map(_.split("\\W+").map(_.toLowerCase)) // transform each file into array of words
      .map(arr => stemmer.stem(arr.zipWithIndex)) // stem words in files
      .map(countWords) // transforms words into Map of word -> occurences
      .map(mapToIndex) // transform arrays to sparse
  }

  def rddToIndexedRows(rdd: RDD[(String, String)]): RDD[IndexedRow] = indexRDD(rdd.map(_._2))
    .zipWithIndex()
    .map { case (vector, index) => IndexedRow(index, vector) }
}
