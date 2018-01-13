
class AppConfig(conf: ConfigBuilder) {
  val textDirectory:String =conf.textDirectory
  val bagOfWordsStorage:String =conf.bagOfWordsStorage
  val indexStorage: String =conf.indexStorage
  val isBagOfWordsStored: Boolean =conf.isBagOfWordsStored
  val isIndexStored: Boolean =conf.isIndexStored

}


