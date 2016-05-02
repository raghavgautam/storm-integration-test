package com.hortonworks.storm.st;

import org.apache.storm.generated.StormTopology;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

/**
 * Created by temp on 5/2/16.
 */
public abstract class AbstractTest {
    final StormCluster cluster = new StormCluster();
    final String topologyName = this.getClass().getSimpleName();
    final TopoWrap topo = new TopoWrap(cluster, topologyName, getTopology());

    @BeforeTest
    public void setup() {
    }

    protected abstract StormTopology getTopology();

    @AfterTest
    public void tearDown() throws Exception {
        topo.killQuietly();
        AssertUtil.empty(cluster.getActive());
    }

}
