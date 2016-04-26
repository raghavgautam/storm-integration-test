import java.util

import org.apache.storm.StormSubmitter
import org.slf4j.LoggerFactory
import org.apache.storm.generated.{KillOptions, StormTopology}
import org.apache.storm.shade.org.json.simple.JSONValue
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.testing.TestWordSpout
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.topology.{OutputFieldsDeclarer, TopologyBuilder}
import org.apache.storm.tuple.{Fields, Tuple, Values}
import org.apache.storm.utils.NimbusClient
import org.apache.storm.utils.Utils
import org.scalatest.testng.TestNGSuiteLike
import org.testng.{Assert, TestNG}
import org.testng.annotations._

import scala.collection.JavaConverters._
import scala.io.StdIn
import scala.reflect.io.File

class MainTest extends TestNG with TestNGSuiteLike {
  /*
  @DataProvider(name = "jsonProvider")
  def jsonProvider(): Array[Array[Object]] = {
    def getJsonFiles: Array[File] = {
      val jsonDir = this.getClass.getResource("json").getFile
      val jsonFilter: Array[String] = Array("json")
      FileUtils.listFiles(new File(jsonDir), jsonFilter, false).asScala.toArray
    }
    def asObject(s: String) = s.asInstanceOf[Object]
    val jsonFiles: Array[String] = getJsonFiles.map(_.getName)
    val clsNames = jsonFiles.map(_.split('.').head.capitalize).map(asObject)
    val jsonStrings = jsonFiles.map(j => Util.getResource("/json/" + j))
    jsonStrings.zip(clsNames).map(p => Array(p._1, p._2))
  }
  */

  //@Test(dataProvider = "jsonProvider")
  //def scalaTest(jsStr:String, clsName: String) = {
  val log = LoggerFactory.getLogger(this.getClass.getSimpleName)
  @Test(enabled = true)
  def scalaTest() = {
    log.error(">" * 80 + "scala")
    val buildJar: String = System.getProperty("buildJar")
    log.error(s"buildJar = $buildJar")
    log.error(s"exists = ${buildJar != null && File(buildJar).exists}")
    val conf: util.Map[_, _] = Utils.readStormConfig()

    val topologyName: String = "TestTopology"
    val client = NimbusClient.getConfiguredClient(conf).getClient
    log.info(s"Cluster info: ${client.getClusterInfo}")
    val jarFile = if (buildJar!=null) buildJar else "/Users/temp/tmp/storm-integration-test-1.0-SNAPSHOT.jar"
    try {
      val jsonConf: String = JSONValue.toJSONString(conf)
      System.setProperty("storm.jar", jarFile)
      StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, getTopology)
      Thread.sleep(60 * 1000)
      log.info("Continuing...")
    } finally {
      try {
        client.killTopologyWithOpts(topologyName, new KillOptions())
        log.info("Topology killed.")
      } catch {
        case _: Throwable => log.warn(s"Couldn't kill topology: $topologyName")
      }
    }
    Assert.assertEquals(true, true, s"Mismatch for case")
  }

  def getTopology: StormTopology = {
    val builder = new TopologyBuilder()
    builder.setSpout("word", new TestWordSpout, 10)
    builder.setBolt("exclaim1", new ExclamationBolt, 3).shuffleGrouping("word")
    builder.setBolt("exclaim2", new ExclamationBolt, 2).shuffleGrouping("exclaim1")
    builder.createTopology()
  }
}

class ExclamationBolt extends BaseRichBolt {
  private var _collector: OutputCollector = null

  def prepare(conf: util.Map[_, _], context: TopologyContext, collector: OutputCollector) {
    _collector = collector
  }

  def execute(tuple: Tuple) {
    _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"))
    _collector.ack(tuple)
  }

  def declareOutputFields(declarer: OutputFieldsDeclarer) {
    declarer.declare(new Fields("word"))
  }
}
