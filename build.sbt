name := "arcos"

version := "0.1"

scalaVersion := "2.11.12"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.1" % "provided"

// https://spark.apache.org/docs/latest/ml-guide.html#dependencies
// https://mvnrepository.com/artifact/com.github.fommil.netlib/all
libraryDependencies += "com.github.fommil.netlib" % "all" % "1.1.2"

// sbt-assembly section
test in assembly := {}
mainClass in assembly := some("io.jekal.arcos.ArcosMain")
assemblyJarName := s"${name.value}_${scalaVersion.value}-${version.value}-uber.jar"

val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.first
}
