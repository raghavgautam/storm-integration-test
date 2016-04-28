package com.hortonworks.storm.st;

import org.apache.storm.ExclamationTopology;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.shade.org.json.simple.JSONValue;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.*;

public class MainTest {
    private Logger log = LoggerFactory.getLogger(this.getClass().getSimpleName());

    @Test
    public void submissionTest() throws TException {
        log.error(StringUtils.repeat(">", 80) + "scala");
        String jarFile = getJarPath();
        Map conf = Utils.readStormConfig();
        String topologyName = "TestTopology";
        Nimbus.Client client = NimbusClient.getConfiguredClient(conf).getClient();
        log.info("Cluster info: " + client.getClusterInfo());
        try {
            String jsonConf = JSONValue.toJSONString(conf);
            log.info("setting storm.jar to: " + jarFile);
            System.setProperty("storm.jar", jarFile);
            Map<String, Object> submitConf = new HashMap<>();
            submitConf.put("storm.zookeeper.topology.auth.scheme", "digest");
            submitConf.put("topology.workers", 3);
            submitConf.put("topology.debug", true);
            StormSubmitter.submitTopologyWithProgressBar(topologyName, submitConf, getTopology());
            try {
                Thread.sleep(30 * 1000);
            } catch (InterruptedException ignore) {
            }
            log.info("Continuing...");
        } finally {
            try {
                client.killTopologyWithOpts(topologyName, new KillOptions());
                log.info("Topology killed.");
            } catch (Throwable e){
                log.warn("Couldn't kill topology: " + topologyName);
            }
        }
        Assert.assertEquals(true, true, "Mismatch for case");
    }

    private String getJarPath() {
        List<String> jarPaths = Arrays.asList(
                System.getProperty("buildJar"),
                "/Users/temp/storm-integration-test/target/storm-integration-test-1.0.1-SNAPSHOT.jar",
                "/Users/temp/tmp/storm-integration-test-1.0.1-SNAPSHOT.jar"
        );
        String jarFile = null;
        for (String jarPath : jarPaths) {
            boolean existsFlag = jarPath != null && new File(jarPath).exists();
            log.debug("jarPath = " + jarPath + " exists = " + existsFlag);
            if (existsFlag) {
                jarFile = jarPath;
                break;
            }
        }
        Assert.assertNotNull(jarFile, "Couldn't detect a suitable jar file for uploading.");
        log.info("jarFile = " + jarFile);
        return jarFile;
    }

    private StormTopology getTopology() {
        return ExclamationTopology.getStormTopology();
    }
}
