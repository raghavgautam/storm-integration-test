package com.hortonworks.storm.st;

import org.apache.storm.ExclamationTopology;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public final class MainTest extends AbstractTest {
    private static Logger log = LoggerFactory.getLogger(MainTest.class);

    protected StormTopology getTopology() {
        return ExclamationTopology.getStormTopology();
    }

    @Test
    public void submissionTest() throws TException {
        AssertUtil.empty(cluster.getSummaries());
        topo.submitSuccessfully();
        for(int i=0; i < 10; ++i) {
            TopologyInfo topologyInfo = topo.getInfo();
            log.info(topologyInfo.toString());
            long wordSpoutEmittedCount = topo.getAllTimeEmittedCount(ExclamationTopology.WORD);
            long exclaim1EmittedCount = topo.getAllTimeEmittedCount(ExclamationTopology.EXCLAIM_1);
            long exclaim2EmittedCount = topo.getAllTimeEmittedCount(ExclamationTopology.EXCLAIM_2);
            log.info("wordSpoutEmittedCount for spout 'word' = " + wordSpoutEmittedCount);
            log.info("exclaim1EmittedCount = " + exclaim1EmittedCount);
            log.info("exclaim2EmittedCount = " + exclaim2EmittedCount);
            if (wordSpoutEmittedCount > 10000 || exclaim2EmittedCount > 1000) {
                break;
            }
            TimeUtil.sleepSec(6);
        }
    }

}
