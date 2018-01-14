
class AppConfig(conf: ConfigBuilder) {
  val textDirectory: String = conf.textDirectory
  val searchEngineStorage: String = conf.searchEngineStorage
  val isDataStored: Boolean = conf.isDataStored
  val urlsDirectory: String = conf.urlsDirectory
  val stopWordsLocation: String = conf.stopWordsLocation
  val persistData: Boolean = conf.persistData
  val rddStorage: String = conf.rddStorage
}


