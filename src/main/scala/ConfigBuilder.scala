import java.io.File

class ConfigBuilder {
  var textDirectory: String = ""
  var searchEngineStorage = "persistence/searchEngine"
  var isDataStored = false
  var stopWordsLocation: String = ""
  var urlsDirectory: String = ""
  var persistData: Boolean = true
  var rddStorage = "persistence/rdd"


  def dataStored(): ConfigBuilder = {
    isDataStored = true
    this
  }

  def NoPersistance(): ConfigBuilder = {
    persistData = false
    this
  }

  def textDirectory(path: String): ConfigBuilder = {
    textDirectory = path
    this
  }

  def rddStorage(path: String): ConfigBuilder = {
    rddStorage = path
    this
  }


  def searchEngineStorage(path: String): ConfigBuilder = {
    searchEngineStorage = path
    this
  }

  def build(): AppConfig = {

    // TODO add validation

    new AppConfig(this)
  }

  def firstLaunchDefaultConfig(): AppConfig = {
    textDirectory = "arts/"
    stopWordsLocation = "stopwords/large_stopwords.txt"
    urlsDirectory = "urls/"

    new AppConfig(this)
  }

  def nextLaunchDefaultConfig(): AppConfig = {
    isDataStored = true
    new AppConfig(this)
  }

  def autoConfigBuild(): AppConfig = {
    if (new File(rddStorage).exists() && new File(searchEngineStorage).exists())
      nextLaunchDefaultConfig()
    else firstLaunchDefaultConfig()
  }

  case class NotSufficientInfoProvided() extends Throwable


}
