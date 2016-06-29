name := "learnSpark"

version := "1.0"

scalaVersion := "2.11.7"

scalacOptions += "-feature"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.2"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "1.6.2"

libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.3.0"

resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies += "com.github.eseifert" % "vectorgraphics2d" % "0.9.2"

libraryDependencies += "org.knowm.xchart" % "xchart" % "3.0.1" exclude("de.erichseifert.vectorgraphics2d", "VectorGraphics2D") withSources()

resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies += "com.github.kindlychung" % "orson-charts" % "snapshot0.1"

libraryDependencies += "com.github.scala-incubator.io" %% "scala-io-core" % "0.4.3"

libraryDependencies += "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.3"

libraryDependencies ++= Seq(
//  "org.apache.commons" % "commons-lang3" % "3.4",
//  "org.eclipse.jetty" % "jetty-client" % "9.3.7.v20160115",
//  "com.typesafe.play" % "play-json_2.11" % "2.4.6",
//  "org.elasticsearch" % "elasticsearch-hadoop-mr" % "2.1.2",
  "net.sf.opencsv" % "opencsv" % "2.3",
//  "com.twitter.elephantbird" % "elephant-bird" % "4.12",
//  "com.twitter.elephantbird" % "elephant-bird-core" % "4.12",
//  "mysql" % "mysql-connector-java" % "5.1.38",
  "org.scalatest" %% "scalatest" % "2.2.6"
)

resolvers ++= Seq(
  "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
  "Spray Repository" at "http://repo.spray.cc/",
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
  "scala-tools" at "https://oss.sonatype.org/content/groups/scala-tools",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
  Resolver.sonatypeRepo("public")
)
