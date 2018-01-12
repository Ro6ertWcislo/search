name := "search"

version := "1.0"

scalaVersion := "2.10.7"

libraryDependencies += "junit" % "junit" % "4.10" % "test"

libraryDependencies ++= Seq(
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.0.0",
  "org.apache.spark" %% "spark-core" % "2.0.0",
  "org.apache.spark" %% "spark-sql" % "2.0.0",
  "org.apache.spark" %% "spark-mllib" % "2.0.0",
  "com.github.master" %% "spark-stemming" % "0.2.0"
)
libraryDependencies += "nl.razko" %% "scraper" % "0.4.1"
libraryDependencies += "org.jsoup" % "jsoup" % "1.7.2"
