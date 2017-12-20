/**
  * Created by boris on 7/14/17.
  */
import Versions._
import sbt._

object Dependencies {

  val beamFlink     = "org.apache.beam"   % "beam-runners-flink_2.10"                 % beamVersion
  val beamJava      = "org.apache.beam"   % "beam-runners-direct-java"                % beamVersion
  val beamKafka     = "org.apache.beam"   % "beam-sdks-java-io-kafka"                 % beamVersion
  val beamJoin      = "org.apache.beam"   % "beam-sdks-java-extensions-join-library"  % beamVersion
  val beamAPI       = "org.apache.beam"   % "beam-sdks-common-fn-api"                 % beamVersion


  val kafka         = "org.apache.kafka"  % "kafka_2.10"                              % kafkaVersion
  val kafkaclients  = "org.apache.kafka"  % "kafka-clients"                           % kafkaVersion
  val curator       = "org.apache.curator"              % "curator-test"              % CuratorVersion                 // ApacheV2


  val tensorflow    = "org.tensorflow"    % "tensorflow"                              % tensorflowVersion

  val jpmml         = "org.jpmml"         % "pmml-evaluator"                          % jpmmlVersion
  val jpmmlextras   = "org.jpmml"         % "pmml-evaluator-extension"                % jpmmlVersion

  val beamDependencies      = Seq(beamAPI, beamJoin, beamJava, beamKafka, beamFlink)
  val modelDependencies     = Seq(jpmml, jpmmlextras, tensorflow)
  val kafkaDependencies     = Seq(kafka, kafkaclients)
}
