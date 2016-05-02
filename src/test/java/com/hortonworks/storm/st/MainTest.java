package com.hortonworks.storm.st;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.ExclamationTopology;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.Map;

public class MainTest {
    private static Logger log = LoggerFactory.getLogger(MainTest.class);

    @Test
    public void submissionTest() throws TException {
        log.error(StringUtils.repeat(">", 80) + "scala");
        String topologyName = "TestTopology";
        Nimbus.Client client = getNimbusClient();
        AssertUtil.empty(TopologyUtils.getSummaries(client));
        TopoWrap topo = new TopoWrap(client, topologyName, getTopology());
        topo.submit();
        try {
            for(int i=0; i < 10; ++i) {
                TopologyInfo topologyInfo = topo.getInfo();
                log.info(topologyInfo.toString());
                long spoutEmittedCount = topo.getAllTimeEmittedCount("word");
                long exclaim1EmittedCount = topo.getAllTimeEmittedCount("exclaim1");
                long exclaim2EmittedCount = topo.getAllTimeEmittedCount("exclaim2");
                log.info("spoutEmittedCount for spout 'word' = " + spoutEmittedCount);
                log.info("exclaim1EmittedCount = " + exclaim1EmittedCount);
                log.info("exclaim2EmittedCount = " + exclaim2EmittedCount);
                if (spoutEmittedCount > 10000 || exclaim2EmittedCount > 1000) {
                    break;
                }
                TimeUtil.sleepSec(6);
            }
            log.info("Continuing...");
        } finally {
            topo.killQuietly();
            AssertUtil.nonEmpty(TopologyUtils.getKilled(client));
        }
    }

    private Nimbus.Client getNimbusClient() {
        Map conf = Utils.readStormConfig();
        return NimbusClient.getConfiguredClient(conf).getClient();
    }

    private StormTopology getTopology() {
        return ExclamationTopology.getStormTopology();
    }
}
