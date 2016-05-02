package com.hortonworks.storm.st;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import org.apache.storm.generated.*;
import org.apache.storm.thrift.TException;
import org.testng.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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

    public long getAllTimeEmittedCount(final String componentId) throws TException {
        TopologyInfo info = getInfo();
        final List<ExecutorSummary> executors = info.get_executors();
        List<Long> ackCounts = Lists.transform(executors, new Function<ExecutorSummary, Long>() {
            @Nullable
            @Override
            public Long apply(@Nullable ExecutorSummary input) {
                if (input == null || !input.get_component_id().equals(componentId))
                    return 0L;
                String since = ":all-time";
                return getEmittedCount(input, since);
            }

            //possible values for since are strings :all-time, 600, 10800, 86400
            public Long getEmittedCount(@Nonnull ExecutorSummary input, @Nonnull String since) {
                ExecutorStats executorStats = input.get_stats();
                if (executorStats == null)
                    return 0L;
                Map<String, Map<String, Long>> emitted = executorStats.get_emitted();
                if (emitted == null)
                    return 0L;
                Map<String, Long> allTime = emitted.get(since);
                if (allTime == null)
                    return 0L;
                return allTime.get("default");
            }
        });
        return sum(ackCounts).longValue();
    }

    private Number sum(Collection<? extends Number> nums) {
        Double retVal = 0.0;
        for (Number num : nums) {
            if(num != null) {
                retVal += num.doubleValue();
            }
        }
        return retVal;
    }

    public void killQuietly() {
        TopologyUtils.killSilently(name, client);
    }
}
