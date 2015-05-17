lazy val root = (project in file(".")).
  settings(
    name := "MovesCount",
    version := "0.0.1",
    scalaVersion := "2.10.4",
    mainClass in Compile := Some("de.ripke.movescount.scala.MovesCount")        
  )

libraryDependencies ++= Seq(
 		"org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
  		"org.apache.spark" %% "spark-sql" % "1.3.0" % "provided",
  		"org.scalaj" %% "scalaj-http" % "1.1.4"
)
