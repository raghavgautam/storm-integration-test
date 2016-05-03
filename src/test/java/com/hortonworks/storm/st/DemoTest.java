package com.hortonworks.storm.st;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.hortonworks.storm.st.helper.AbstractTest;
import com.hortonworks.storm.st.utils.AssertUtil;
import com.hortonworks.storm.st.utils.TimeUtil;
import org.apache.storm.ExclamationTopology;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.TopologyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.net.URL;
import java.util.Collection;
import java.util.List;

public final class DemoTest extends AbstractTest {
    private static Logger log = LoggerFactory.getLogger(DemoTest.class);
    private static Collection<String> words = Lists.newArrayList("nathan", "mike", "jackson", "golda", "bertels");
    private static Collection<String> exclaim2Oputput = Collections2.transform(words, new Function<String, String>() {
        @Nullable
        @Override
        public String apply(@Nullable String input) {
            return input +  "!!!!!!";
        }
    });

    protected StormTopology getTopology() {
        return ExclamationTopology.getStormTopology();
    }

    @Test
    public void testExclamationTopology() throws Exception {
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
        List<URL> boltUrls = topo.getLogUrls(ExclamationTopology.WORD);
        log.info(boltUrls.toString());
        final String actualOutput = topo.getLogs(ExclamationTopology.EXCLAIM_2);
        for (String oneExpectedOutput : exclaim2Oputput) {
            Assert.assertTrue(actualOutput.contains(oneExpectedOutput), "Couldn't find " + oneExpectedOutput + " in urls");
        }
        Assert.assertTrue(exclaim2EmittedCount >= minExclaim2Emits, "Expecting at least " + minExclaim2Emits + " exclaim2 messages by now found = " + exclaim2EmittedCount);
    }
}
