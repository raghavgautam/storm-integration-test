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

    public static void submit(String topologyName, StormTopology topology) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        Map<String, Object> submitConf = getSubmitConf();
        String jarFile = getJarPath();
        log.info("setting storm.jar to: " + jarFile);
        System.setProperty("storm.jar", jarFile);
        StormSubmitter.submitTopologyWithProgressBar(topologyName, submitConf, topology);
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

    public static void killSilently(String topologyName, Nimbus.Client client) {
        try {
            client.killTopologyWithOpts(topologyName, new KillOptions());
            log.info("Topology killed: " + topologyName);
        } catch (Throwable e){
            log.warn("Couldn't kill topology: " + topologyName + " Exception: " + ExceptionUtils.getFullStackTrace(e));
        }
    }
}
