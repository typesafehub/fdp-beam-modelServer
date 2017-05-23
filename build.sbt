
name := "BeamModelServer"

version := "1.0"

scalaVersion := "2.10.6"

lazy val flinkBeamRunner = "0.6.0"
lazy val kafka = "0.10.0.1"

lazy val tensorflow = "1.1.0"
lazy val PMML = "1.3.5"

PB.targets in Compile := Seq(
  PB.gens.java -> (sourceManaged in Compile).value,
  scalapb.gen(javaConversions=true) -> (sourceManaged in Compile).value
)

// dependencies

libraryDependencies ++= Seq(
  "org.apache.beam" % "beam-runners-flink_2.10" % flinkBeamRunner,
//  "org.apache.flink" % "flink-connector-kafka-0.10_2.10" % flinkVersion,
  "org.apache.beam" % "beam-runners-direct-java" % flinkBeamRunner,
  "org.apache.beam" % "beam-sdks-java-io-kafka" % flinkBeamRunner,
  "org.apache.curator" % "curator-test" % "3.2.0",
  "org.apache.kafka" % "kafka_2.10" % kafka,
  "org.apache.kafka" % "kafka-clients" % kafka,
  "org.tensorflow" % "tensorflow" % tensorflow,
  "org.jpmml" % "pmml-evaluator" % PMML,
  "org.jpmml" % "pmml-evaluator-extension" % PMML
)
        