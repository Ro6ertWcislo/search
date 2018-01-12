import scala.io.Source

class StopWords(val path: String) extends Serializable{
  val stopWords: Set[String] = parse(path)
  def parse(path:String):Set[String] = {
    Source.fromFile(path)
      .getLines()
      .toSet
      .flatMap((x:String) => x.split(" "))
  }
  def get(): Set[String] = stopWords
  def notContains(word: String):Boolean = !stopWords.contains(word)
}
