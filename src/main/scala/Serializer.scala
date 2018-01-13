import java.io._

import SparkConf.{appConf, sc}
import org.apache.spark.rdd.RDD
class Serializer {

  def serialize(obj: Any):Unit = obj match {
    case searchEngine: SearchEngine => serialize(searchEngine,appConf.searchEngineStorage)
    case rdd: RDD[AnyRef] => rdd.saveAsObjectFile(appConf.rddStorage)
    case _ => throw new IllegalArgumentException
  }
  def serialize(obj: Any,path:String):Unit = {
    val file = new File(path)
    file.createNewFile()
    val oos = new ObjectOutputStream(new FileOutputStream(file))
    oos.writeObject(obj)
    oos.close()
  }

  def deserialize[T](path: String): T = {
    val ois = new ObjectInputStream(new FileInputStream(path))
    val obj = ois.readObject.asInstanceOf[T]
    ois.close()
    obj
  }

}
