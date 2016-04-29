package com.hortonworks.storm.st;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import org.apache.storm.generated.*;
import org.apache.storm.thrift.TException;
import org.testng.Assert;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

/**
 * Created by temp on 4/29/16.
 */
public class TopoWrap {
    private final Nimbus.Client client;
    private final String name;
    private final StormTopology topology;
    private String id;

    public TopoWrap(Nimbus.Client nimbusClient, String name, StormTopology topology) {
        this.client = nimbusClient;
        this.name = name;
        this.topology = topology;
    }

    public void submit() throws TException {
        TopologyUtils.submit(name, topology);
        TopologySummary topologySummary = getSummary();
        Assert.assertEquals(topologySummary.get_status().toLowerCase(), "active", "Topology must be active.");
        id = topologySummary.get_id();
    }

    private TopologySummary getSummary() throws TException {
        List<TopologySummary> allTopos = TopologyUtils.getSummaries(client);
        Collection<TopologySummary> oneTopo = Collections2.filter(allTopos, new Predicate<TopologySummary>() {
            @Override
            public boolean apply(@Nullable TopologySummary input) {
                return input != null && input.get_name().equals(name);
            }
        });
        AssertUtil.assertOneElement(oneTopo);
        return oneTopo.iterator().next();
    }

    public TopologyInfo getInfo() throws TException {
        return client.getTopologyInfo(id);
    }

    public void killQuietly() {
        TopologyUtils.killSilently(name, client);
    }
}
