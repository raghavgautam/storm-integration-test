package com.hortonworks.storm.st.helper;

import com.hortonworks.storm.st.utils.AssertUtil;
import com.hortonworks.storm.st.wrapper.StormCluster;
import com.hortonworks.storm.st.wrapper.TopoWrap;
import org.apache.storm.generated.StormTopology;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

/**
 * Created by temp on 5/2/16.
 */
public abstract class AbstractTest {
    protected final StormCluster cluster = new StormCluster();
    final String topologyName = this.getClass().getSimpleName();
    protected final TopoWrap topo = new TopoWrap(cluster, topologyName, getTopology());

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
