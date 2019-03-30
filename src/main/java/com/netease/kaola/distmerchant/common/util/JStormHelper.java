package com.netease.kaola.distmerchant.common.util;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.Map.Entry;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.alibaba.jstorm.utils.JStormUtils;
import com.netease.kaola.distmerchant.common.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.callback.Callback;
import com.alibaba.jstorm.cluster.Cluster;

import com.alibaba.jstorm.cluster.StormConfig;
import com.alibaba.jstorm.cluster.StormZkClusterState;
import com.alibaba.jstorm.metric.MetaType;
import com.alibaba.jstorm.metric.MetricDef;
import com.alibaba.jstorm.task.error.ErrorConstants;
import com.alibaba.jstorm.task.error.TaskError;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.NimbusClientWrapper;
import backtype.storm.utils.Utils;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.util.ResourceUtils;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

public final class JStormHelper {
    private final static Logger LOG = LoggerFactory.getLogger(JStormHelper.class);

    private static NimbusClient client = null;

    private JStormHelper() {
    }

    public static void runTopologyLocally(StormTopology topology, String topologyName, Config conf,
                                          int runtimeInSeconds, Callback callback) throws Exception {
        LocalCluster cluster = new LocalCluster();
        conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
        cluster.submitTopology(topologyName, conf, topology);

        if (runtimeInSeconds < 120) {
            JStormUtils.sleepMs(120 * 1000);
        } else {
            JStormUtils.sleepMs(runtimeInSeconds * 1000);
        }

        if (callback != null) {
            callback.execute(topologyName);
        }
        while (true) {
            Thread.sleep(10);
        }
//        cluster.killTopology(topologyName);
//        cluster.shutdown();
    }

    public static void runTopologyRemotely(StormTopology topology, String topologyName, Config conf,
                                           int runtimeInSeconds, Callback callback) throws Exception {
        if (conf.get(Config.TOPOLOGY_WORKERS) == null) {
            conf.setNumWorkers(1);
        }

        StormSubmitter.submitTopology(topologyName, conf, topology);

//        if (JStormUtils.parseBoolean(conf.get("RUN_LONG_TIME"), false)) {
//            LOG.info(topologyName + " will run long time");
//            return;
//        }
//
//        if (runtimeInSeconds < 120) {
//            JStormUtils.sleepMs(120 * 1000);
//        } else {
//            JStormUtils.sleepMs(runtimeInSeconds * 1000);
//        }
//
//        if (callback != null) {
//            callback.execute(topologyName);
//        }

//        killTopology(conf, topologyName);
    }

    public static void runTopology(StormTopology topology, String topologyName, Config conf, int runtimeInSeconds,
                                   Callback callback, boolean isLocal) throws Exception {
        if (isLocal) {
            runTopologyLocally(topology, topologyName, conf, runtimeInSeconds, callback);

        } else {
            runTopologyRemotely(topology, topologyName, conf, runtimeInSeconds, callback);
        }
    }


    public static void killTopology(Map conf, String topologyName) throws Exception {
        NimbusClientWrapper client = new NimbusClientWrapper();
        try {
            Map clusterConf = Utils.readStormConfig();
            clusterConf.putAll(conf);
            client.init(clusterConf);
            KillOptions killOption = new KillOptions();
            killOption.set_wait_secs(1);
            client.getClient().killTopologyWithOpts(topologyName, killOption);
        } finally {
            client.cleanup();
        }
    }

    @Deprecated
    public static Map getFullConf(Map conf) {
        Map realConf = new HashMap();
        boolean isLocal = StormConfig.try_local_mode(conf);
        if (isLocal) {
            realConf.putAll(LocalCluster.getInstance().getLocalClusterMap().getConf());

        } else {
            realConf.putAll(Utils.readStormConfig());
        }
        realConf.putAll(conf);

        return realConf;
    }

    /**
     * This function bring some hacks to JStorm, this isn't a good way
     */
    @Deprecated
    public static StormZkClusterState mkStormZkClusterState(Map conf) throws Exception {
        Map realConf = getFullConf(conf);

        return new StormZkClusterState(realConf);
    }

    @Deprecated
    public static Map<Integer, List<TaskError>> getTaskErrors(String topologyId, Map conf) throws Exception {
        StormZkClusterState clusterState = null;
        try {
            clusterState = mkStormZkClusterState(conf);
            return Cluster.get_all_task_errors(clusterState, topologyId);
        } finally {
            if (clusterState != null) {
                clusterState.disconnect();
            }
        }

    }


    public static void checkError(Map conf, String topologyName) throws Exception {
        NimbusClientWrapper client = new NimbusClientWrapper();
        try {
            Map clusterConf = Utils.readStormConfig();
            clusterConf.putAll(conf);
            client.init(clusterConf);

            String topologyId = client.getClient().getTopologyId(topologyName);

            Map<Integer, List<TaskError>> errors = getTaskErrors(topologyId, conf);

            for (Entry<Integer, List<TaskError>> entry : errors.entrySet()) {
                Integer taskId = entry.getKey();
                List<TaskError> errorList = entry.getValue();
                for (TaskError error : errorList) {
                    if (ErrorConstants.ERROR.equals(error.getLevel())) {
                        Assert.fail(taskId + " occur error:" + error.getError());
                    } else if (ErrorConstants.FATAL.equals(error.getLevel())) {
                        Assert.fail(taskId + " occur error:" + error.getError());
                    }
                }
            }
        } finally {
            client.cleanup();
        }
    }

    public static class CheckAckedFail implements Callback {

        private Map conf;

        public CheckAckedFail(Map conf) {
            this.conf = conf;
        }

        @Override
        public <T> Object execute(T... args) {
            String topologyName = (String) args[0];

            try {
                checkError(conf, topologyName);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            Map stormConf = Utils.readStormConfig();
            stormConf.putAll(conf);
            Map<String, Double> ret = JStormUtils.getMetrics(stormConf, topologyName, MetaType.TASK, null);
            for (Entry<String, Double> entry : ret.entrySet()) {
                String key = entry.getKey();
                Double value = entry.getValue();

                if (key.indexOf(MetricDef.FAILED_NUM) > 0) {
                    Assert.assertTrue(key + " fail number should == 0", value == 0);
                } else if (key.indexOf(MetricDef.ACKED_NUM) > 0) {
                    if (value == 0.0) {
                        LOG.warn(key + ":" + value);
                    } else {
                        LOG.info(key + ":" + value);
                    }

                } else if (key.indexOf(MetricDef.EMMITTED_NUM) > 0) {
                    if (value == 0.0) {
                        LOG.warn(key + ":" + value);
                    } else {
                        LOG.info(key + ":" + value);
                    }
                }
            }

            return ret;
        }
    }

    public static Config loadConf() {
        Config resultConfig = new Config();
//        String location = JStormHelper.class.getProtectionDomain().getCodeSource().getLocation().getPath();
//        boolean isJar = location.endsWith("jar");
//        if (isJar) {
//            ZipFile zipFile = null;
//            try {
//                zipFile = new ZipFile(location);
//                Enumeration zipEntries = zipFile.entries();
//                while (zipEntries.hasMoreElements()) {
//                    ZipEntry zipEntry = (ZipEntry)zipEntries.nextElement();
//                    String name = zipEntry.getName();
//                    //只读取zip一级目录下的配置文件
//                    if (!zipEntry.isDirectory() && name.indexOf("/") <= 0) {
//                        if ("defaults.yaml".equals(name)) {
//                            //排除storm默认配置文件
//                            continue;
//                        }
//                        Map map = null;
//                        if (!name.startsWith("/")) {
//                            name = "/" + name;
//                        }
//                        if (name.matches(".*\\.yaml")) {
//                            InputStream resourceAsStream = JStormHelper.class.getResourceAsStream(name);
//                            map = readYaml(resourceAsStream);
//                        } else if (name.matches(".*\\.properties")) {
//                            InputStream resourceAsStream = JStormHelper.class.getResourceAsStream(name);
//                            map = readProperties(resourceAsStream);
//
//                        }
//                        if (map != null && !map.isEmpty()) {
//                            resultConfig.putAll(map);
//                        }
//                    }
//                }
//            } catch (Exception e) {
//                throw new RuntimeException("run as jar read yaml conf Exception", e);
//            }finally {
//                if (null != zipFile) {
//                    try {
//                        zipFile.close();
//                    } catch (Exception e) {
//                        throw new RuntimeException("close zipfile Exception", e);
//                    }
//                }
//            }
//
//        } else {
            try {
                PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
                Resource[] yamlResources = resolver.getResources( ResourcePatternResolver.CLASSPATH_URL_PREFIX + Constants.RESOURCE_CONF_PATH + "*.yaml");
                if (yamlResources != null && yamlResources.length > 0) {
                    for (Resource resource : yamlResources) {
                        Map map = readYaml(resource.getInputStream());
                        if (map != null && !map.isEmpty()) {
                            resultConfig.putAll(map);
                        }
                    }
                }

                Resource[] propsResources = resolver.getResources( ResourcePatternResolver.CLASSPATH_URL_PREFIX  + Constants.RESOURCE_CONF_PATH + "*.properties");
                if (propsResources != null && propsResources.length > 0) {
                    for (Resource resource : propsResources) {
                        Map map = readProperties(resource.getInputStream());
                        if (map != null && !map.isEmpty()) {
                            resultConfig.putAll(map);
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
//        }
        return resultConfig;
    }

    private static Map readYaml(InputStream inputStream) {
        Yaml yaml = new Yaml(new SafeConstructor());
        Map resMap = (Map) yaml.load(new InputStreamReader(inputStream));
        return resMap;
    }

    private static Map readProperties(InputStream inputStream) throws IOException {
        Map map = new HashMap();
        Properties properties = new Properties();
        properties.load(inputStream);
        if (!properties.isEmpty()) {
            map.putAll(properties);
        }
        return map;
    }
    public static Map loadPropertiesConf() throws IOException {
        Map resultMap = new HashMap();
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] propertieResources = resolver.getResources("classpath*:*.properties");
        if (propertieResources != null && propertieResources.length > 0) {
            for (Resource resource : propertieResources) {
                InputStream in = null;
                Properties properties = new Properties();
                try {
                    in = resource.getInputStream();
                    properties.load(in);
                    resultMap.putAll(properties);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to read properties config file " + resource.getFilename(), e);
                } finally {
                    if (null != in) {
                        try {
                            in.close();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }
        return resultMap;
    }

    public static void main(String[] args) throws IOException {
        Config config =  loadConf();
        System.out.println(config);
    }
}
