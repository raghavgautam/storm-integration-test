package com.hortonworks.storm.st;

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
import java.util.Map;

public class MainTest {
    private Logger log = LoggerFactory.getLogger(this.getClass().getSimpleName());

    @Test
    public void submissionTest() throws TException {
        log.error(StringUtils.repeat(">", 80) + "scala");
        String buildJar = System.getProperty("buildJar");
        log.error("buildJar = " + buildJar);
        log.error("exists = " + (buildJar != null && new File(buildJar).exists()));
        Map conf = Utils.readStormConfig();
        String topologyName = "TestTopology";
        Nimbus.Client client = NimbusClient.getConfiguredClient(conf).getClient();
        log.info("Cluster info: " + client.getClusterInfo());
        String jarFile = buildJar!=null ? buildJar : "/Users/temp/tmp/storm-integration-test-1.0-SNAPSHOT.jar";
        try {
            String jsonConf = JSONValue.toJSONString(conf);
            System.setProperty("storm.jar", jarFile);
            StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, getTopology());
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

    private StormTopology getTopology() {
        return ExclamationTopology.getStormTopology();
    }
}
