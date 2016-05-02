package com.hortonworks.storm.st.wrapper;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.hortonworks.storm.st.utils.AssertUtil;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.storm.generated.*;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import javax.annotation.Nullable;
import java.util.*;

/**
 * Created by temp on 4/28/16.
 */
public class StormCluster {
    private static Logger log = LoggerFactory.getLogger(StormCluster.class);
    private final Nimbus.Client client;

    public StormCluster() {
        Map conf = Utils.readStormConfig();
        this.client = NimbusClient.getConfiguredClient(conf).getClient();
    }

    public List<TopologySummary> getSummaries() throws TException {
        final ClusterSummary clusterInfo = client.getClusterInfo();
        log.info("Cluster info: " + clusterInfo);
        return clusterInfo.get_topologies();
    }

    public List<TopologySummary> getActive() throws TException {
        return getTopologiesWithStatus("active");
    }

    public List<TopologySummary> getKilled() throws TException {
        return getTopologiesWithStatus("killed");
    }

    private List<TopologySummary> getTopologiesWithStatus(final String expectedStatus) throws TException {
        Collection<TopologySummary> topologySummaries = getSummaries();
        Collection<TopologySummary> filteredSummary = Collections2.filter(topologySummaries, new Predicate<TopologySummary>() {
            @Override
            public boolean apply(@Nullable TopologySummary input) {
                return input != null && input.get_status().toLowerCase().equals(expectedStatus.toLowerCase());
            }
        });
        return new ArrayList<>(filteredSummary);
    }

    public void killSilently(String topologyName) {
        try {
            client.killTopologyWithOpts(topologyName, new KillOptions());
            log.info("Topology killed: " + topologyName);
        } catch (Throwable e){
            log.warn("Couldn't kill topology: " + topologyName + " Exception: " + ExceptionUtils.getFullStackTrace(e));
        }
    }

    public TopologySummary getOneActive() throws TException {
        List<TopologySummary> topoSummaries = getActive();
        AssertUtil.nonEmpty(topoSummaries);
        Assert.assertEquals(topoSummaries.size(), 1, "Expected one topology to be running, found: " + topoSummaries);
        return topoSummaries.get(0);
    }

    public TopologyInfo getInfo(TopologySummary topologySummary) throws TException {
        return client.getTopologyInfo(topologySummary.get_id());
    }

    public Nimbus.Client getNimbusClient() {
        return client;
    }
}
