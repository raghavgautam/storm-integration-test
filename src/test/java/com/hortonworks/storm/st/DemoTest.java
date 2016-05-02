package com.hortonworks.storm.st;

import com.hortonworks.storm.st.helper.AbstractTest;
import com.hortonworks.storm.st.utils.AssertUtil;
import com.hortonworks.storm.st.utils.TimeUtil;
import org.apache.storm.ExclamationTopology;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public final class DemoTest extends AbstractTest {
    private static Logger log = LoggerFactory.getLogger(DemoTest.class);

    protected StormTopology getTopology() {
        return ExclamationTopology.getStormTopology();
    }

    @Test
    public void runExclamationTopology() throws TException {
        AssertUtil.empty(cluster.getSummaries());
        topo.submitSuccessfully();
        long exclaim2EmittedCount = 0;
        long wordSpoutEmittedCount = 0;
        int minExclaim2Emits = 1000;
        for(int i = 0; i < 10 && exclaim2EmittedCount < minExclaim2Emits && wordSpoutEmittedCount < 10000; ++i) {
            TopologyInfo topologyInfo = topo.getInfo();
            log.info(topologyInfo.toString());
            wordSpoutEmittedCount = topo.getAllTimeEmittedCount(ExclamationTopology.WORD);
            long exclaim1EmittedCount = topo.getAllTimeEmittedCount(ExclamationTopology.EXCLAIM_1);
            exclaim2EmittedCount = topo.getAllTimeEmittedCount(ExclamationTopology.EXCLAIM_2);
            log.info("wordSpoutEmittedCount for spout 'word' = " + wordSpoutEmittedCount);
            log.info("exclaim1EmittedCount = " + exclaim1EmittedCount);
            log.info("exclaim2EmittedCount = " + exclaim2EmittedCount);
            TimeUtil.sleepSec(6);
        }
        Assert.assertTrue(exclaim2EmittedCount >= minExclaim2Emits, "Expecting at least 1000 exclaim2 messages by now.");
    }
}
