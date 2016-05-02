package com.hortonworks.storm.st;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.*;
import org.apache.storm.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by temp on 4/29/16.
 */
public class TopoWrap {
    private static Logger log = LoggerFactory.getLogger(TopoWrap.class);
    private final StormCluster cluster;
    private final String name;
    private final StormTopology topology;
    private String id;

    public TopoWrap(StormCluster cluster, String name, StormTopology topology) {
        this.cluster = cluster;
        this.name = name;
        this.topology = topology;
    }

    public void submit() throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        Map<String, Object> submitConf = getSubmitConf();
        String jarFile = getJarPath();
        log.info("setting storm.jar to: " + jarFile);
        System.setProperty("storm.jar", jarFile);
        StormSubmitter.submitTopologyWithProgressBar(name, submitConf, topology);
    }

    private static Map<String, Object> getSubmitConf() {
        Map<String, Object> submitConf = new HashMap<>();
        submitConf.put("storm.zookeeper.topology.auth.scheme", "digest");
        submitConf.put("topology.workers", 3);
        submitConf.put("topology.debug", true);
        return submitConf;
    }

    private static String getJarPath() {
        final String USER_DIR = "user.dir";
        String userDirVal = System.getProperty(USER_DIR);
        Assert.assertNotNull(userDirVal, "property " + USER_DIR + " was not set.");
        File projectDir = new File(userDirVal);
        AssertUtil.exists(projectDir);
        Collection<File> jarFiles = FileUtils.listFiles(projectDir, new String[]{"jar"}, true);
        log.debug("Found jar files: " + jarFiles);
        AssertUtil.nonEmpty(jarFiles);
        String jarFile = null;
        for (File jarPath : jarFiles) {
            log.debug("jarPath = " + jarPath);
            if (jarPath != null && !jarPath.getPath().contains("original")) {
                AssertUtil.exists(jarPath);
                jarFile = jarPath.getAbsolutePath();
                break;
            }
        }
        Assert.assertNotNull(jarFile, "Couldn't detect a suitable jar file for uploading.");
        log.info("jarFile = " + jarFile);
        return jarFile;
    }

    public void submitSuccessfully() throws TException {
        submit();
        TopologySummary topologySummary = getSummary();
        Assert.assertEquals(topologySummary.get_status().toLowerCase(), "active", "Topology must be active.");
        id = topologySummary.get_id();
    }

    private TopologySummary getSummary() throws TException {
        List<TopologySummary> allTopos = cluster.getSummaries();
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
        return cluster.getNimbusClient().getTopologyInfo(id);
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
        cluster.killSilently(name);
    }
}
