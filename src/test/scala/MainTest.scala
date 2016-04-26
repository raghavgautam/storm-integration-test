import java.util

import org.apache.log4j.Logger
import org.apache.storm.generated.KillOptions
import org.apache.storm.shade.org.json.simple.JSONValue
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.utils.NimbusClient
import org.apache.storm.utils.Utils
import org.scalatest.testng.TestNGSuiteLike
import org.testng.{Assert, TestNG}
import org.testng.annotations._

import scala.collection.JavaConverters._
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
  val log = Logger.getLogger(this.getClass.getSimpleName)
  @Test(enabled = true)
  def scalaTest() = {
    log.error(">" * 80 + "scala")
    val buildJar: String = System.getProperty("buildJar")
    log.error(s"buildJar = $buildJar")
    log.error(s"exists = ${File(buildJar).exists}")
    val conf: util.Map[_, _] = Utils.readStormConfig()
    val topologyBuilder = new TopologyBuilder()
    val topologyName: String = "TestTopology"
    //StormSubmitter.submitTopology(topologyName, conf, topologyBuilder.createTopology())
    val client = NimbusClient.getConfiguredClient(conf).getClient
    log.info(s"Cluster info: ${client.getClusterInfo}")
    val jarFile = "C:\\workspace\\TestStormRunner\\target\\TestStormRunner-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
    try {
      val jsonConf: String = JSONValue.toJSONString(conf)
      //http://nishutayaltech.blogspot.in/2014/06/submitting-topology-to-remote-storm.html
      client.submitTopology(topologyName, jarFile, jsonConf, topologyBuilder.createTopology())
    } finally {
      try {
        client.killTopologyWithOpts(topologyName, new KillOptions())
      } catch {
        case _: Throwable => log.warn(s"Couldn't kill topology: $topologyName")
      }
    }
    Assert.assertEquals(true, true, s"Mismatch for case")
  }
}