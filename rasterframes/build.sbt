name := "geotrellis_runner"

scalaVersion := "2.11.8"

lazy val trellis = (project in file("."))

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0" % "provided"
libraryDependencies += "org.locationtech.rasterframes" %% "rasterframes" % "0.7.0"
libraryDependencies += "org.locationtech.rasterframes" %% "rasterframes-datasource" % "0.7.0"
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided"

resolvers += 
"Locationtech" at "https://repo.locationtech.org/content/repositories/releases/"

test in assembly := {}

logBuffered in Test := false

parallelExecution in Test := false

assemblyJarName in assembly := "rasterframes_runner.jar"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly :=  {
    case x if x.endsWith("git.properties") => MergeStrategy.last
    case x => 
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
}
