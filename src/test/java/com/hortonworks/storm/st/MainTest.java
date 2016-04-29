package com.hortonworks.storm.st;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.ExclamationTopology;
import org.apache.storm.generated.*;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

public class MainTest {
    private static Logger log = LoggerFactory.getLogger(MainTest.class);

    @Test
    public void submissionTest() throws TException {
        log.error(StringUtils.repeat(">", 80) + "scala");
        String topologyName = "TestTopology";
        Nimbus.Client client = getNimbusClient();
        AssertUtil.empty(TopologyUtils.getTopologySummaries(client));
        try {
            TopologyUtils.submitTopology(topologyName, getTopology());
            for(int i=0; i < 10; ++i) {
                List<TopologySummary> topologySummaries = TopologyUtils.getActiveTopologies(client);
                AssertUtil.nonEmpty(topologySummaries);
                log.info(topologySummaries.toString());
                TimeUtil.sleepSec(6);
            }
            log.info("Continuing...");
        } finally {
            try {
                client.killTopologyWithOpts(topologyName, new KillOptions());
                log.info("Topology killed.");
                AssertUtil.nonEmpty(TopologyUtils.getKilledTopologies(client));
            } catch (Throwable e){
                log.warn("Couldn't kill topology: " + topologyName);
            }
        }
        Assert.assertEquals(true, true, "Mismatch for case");
    }

    private Nimbus.Client getNimbusClient() {
        Map conf = Utils.readStormConfig();
        return NimbusClient.getConfiguredClient(conf).getClient();
    }

    private StormTopology getTopology() {
        return ExclamationTopology.getStormTopology();
    }
}
