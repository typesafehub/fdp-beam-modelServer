
name := "BeamModelServer"

version := "1.0"

scalaVersion := "2.10.6"

lazy val protobufs = (project in file("./protobufs"))
  .settings(
    PB.targets in Compile := Seq(
      PB.gens.java -> (sourceManaged in Compile).value,
      scalapb.gen(javaConversions=true) -> (sourceManaged in Compile).value
    )
  )

lazy val client = (project in file("./client"))
  .settings(libraryDependencies ++= Dependencies.kafkaDependencies)
  .dependsOn(protobufs, configuration)

lazy val model = (project in file("./model"))
  .settings(libraryDependencies ++= Dependencies.modelDependencies)
  .dependsOn(protobufs)

lazy val server = (project in file("./server"))
  .settings(libraryDependencies ++= Dependencies.kafkaDependencies ++ Dependencies.beamDependencies)
  .dependsOn(model, configuration)

lazy val configuration = (project in file("./configuration"))

lazy val root = (project in file(".")).
  aggregate(protobufs, client, model, configuration, server)