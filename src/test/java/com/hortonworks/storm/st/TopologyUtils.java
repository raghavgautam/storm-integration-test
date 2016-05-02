package com.hortonworks.storm.st;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import javax.annotation.Nullable;
import java.io.File;
import java.util.*;

/**
 * Created by temp on 4/28/16.
 */
public class TopologyUtils {
    private static Logger log = LoggerFactory.getLogger(TopologyUtils.class);

    public static List<TopologySummary> getSummaries(Nimbus.Client client) throws TException {
        final ClusterSummary clusterInfo = client.getClusterInfo();
        log.info("Cluster info: " + clusterInfo);
        return clusterInfo.get_topologies();
    }

    public static List<TopologySummary> getActive(final Nimbus.Client client) throws TException {
        return getTopologiesWithStatus(client, "active");
    }

    public static List<TopologySummary> getKilled(final Nimbus.Client client) throws TException {
        return getTopologiesWithStatus(client, "killed");
    }

    private static List<TopologySummary> getTopologiesWithStatus(final Nimbus.Client client, final String expectedStatus) throws TException {
        Collection<TopologySummary> topologySummaries = getSummaries(client);
        Collection<TopologySummary> filteredSummary = Collections2.filter(topologySummaries, new Predicate<TopologySummary>() {
            @Override
            public boolean apply(@Nullable TopologySummary input) {
                return input != null && input.get_status().toLowerCase().equals(expectedStatus.toLowerCase());
            }
        });
        return new ArrayList<>(filteredSummary);
    }

    public static void killSilently(String topologyName, Nimbus.Client client) {
        try {
            client.killTopologyWithOpts(topologyName, new KillOptions());
            log.info("Topology killed: " + topologyName);
        } catch (Throwable e){
            log.warn("Couldn't kill topology: " + topologyName + " Exception: " + ExceptionUtils.getFullStackTrace(e));
        }
    }

    public static TopologySummary getOneActive(Nimbus.Client client) throws TException {
        List<TopologySummary> topoSummaries = getActive(client);
        AssertUtil.nonEmpty(topoSummaries);
        Assert.assertEquals(topoSummaries.size(), 1, "Expected one topology to be running, found: " + topoSummaries);
        return topoSummaries.get(0);
    }

    public static TopologyInfo getInfo(Nimbus.Client client, TopologySummary topologySummary) throws TException {
        return client.getTopologyInfo(topologySummary.get_id());
    }
}
