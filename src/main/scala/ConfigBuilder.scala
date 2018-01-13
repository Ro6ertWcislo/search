
class ConfigBuilder {
  var textDirectory:String =""
  var searchEngineStorage: String =""
  var isDataStored =false
  var stopWordsLocation: String =""
  var urlsDirectory:String = ""
  var persistData: Boolean=true
  var rddStorage:String = ""


  def dataStored(): ConfigBuilder ={
    isDataStored=true
    this
  }
  def NoPersistance():ConfigBuilder ={
    persistData =false
    this
  }
  def textDirectory(path:String): ConfigBuilder = {
    textDirectory = path
    this
  }
  def rddStorage(path:String): ConfigBuilder = {
    rddStorage = path
    this
  }

  def searchEngineStorage(path:String): ConfigBuilder = {
    searchEngineStorage = path
    this
  }

  def build(): AppConfig ={

    // TODO add validation

    new AppConfig(this)
  }
  def firstLaunchDefaultConfig():AppConfig ={
    textDirectory ="arts/"
    stopWordsLocation ="stopwords/large_stopwords.txt"
    urlsDirectory ="urls/"
    searchEngineStorage = "persistence/searchEngine"
    rddStorage = "persistence/rdd"

    new AppConfig(this)
  }
  def nextLaunchDefaultConfig():AppConfig ={
    isDataStored =true
    searchEngineStorage = "persistence/searchEngine"
    rddStorage = "persistence/rdd"
    new AppConfig(this)

  }

  case class NotSufficientInfoProvided() extends Throwable

}
