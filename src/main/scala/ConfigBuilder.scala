
class ConfigBuilder {
  var textDirectory:String =""
  var bagOfWordsStorage:String =""
  var indexStorage: String =""
  var isBagOfWordsStored =false
  var isIndexStored =false

  def bagOfWordsStored(): ConfigBuilder ={
    isBagOfWordsStored=true
    this
  }
  def indexStored(): ConfigBuilder ={
    isIndexStored=true
    this
  }
  def textDirectory(path:String): ConfigBuilder = {
    textDirectory = path
    this
  }
  def bagOfWordsStorage(path:String): ConfigBuilder = {
    bagOfWordsStorage = path
    this
  }
  def indexStorage(path:String): ConfigBuilder = {
    indexStorage = path
    this
  }

  def build() ={
    if (textDirectory.isEmpty || bagOfWordsStorage.isEmpty || indexStorage.isEmpty)
      throw  NotSufficientInfoProvided()
    new AppConfig(this)
  }

  case class NotSufficientInfoProvided() extends Throwable

}
