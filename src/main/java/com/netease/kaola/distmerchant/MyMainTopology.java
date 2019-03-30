package com.netease.kaola.distmerchant;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import com.netease.kaola.distmerchant.common.Constants;
import com.netease.kaola.distmerchant.common.util.JStormHelper;
import com.netease.kaola.distmerchant.common.util.TopologyUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author kai.zhang
 * @description 分销topology入口
 * @since 2019/1/30
 */
public class MyMainTopology {
    private static final Logger logger = LoggerFactory.getLogger(MyMainTopology.class);
    public static void main(String[] args) throws Exception {
        try {
            //加载topology conf
            Config conf = JStormHelper.loadConf();
            logger.info("################ MyMainTopology conf : {}", conf);
            String clusterModel = (String) conf.get(Constants.STORM_CLUSTER_MODE);
            String topologyName = (String) conf.get(Constants.TOPOLOGY_NAME);
            Integer topologyWorkers = (Integer) conf.get(Constants.TOPOLOGY_WORKERS);
            if (StringUtils.isBlank(topologyName)) {
                throw new RuntimeException(" topology.name is empty");
            }
            if (topologyWorkers == null || topologyWorkers <= 0) {
                //默认一个worker
                conf.put(Constants.TOPOLOGY_WORKERS, 1);
            }
            boolean isLocal = false;
            if ("local".equals(clusterModel)) {
                isLocal = true;
            }
            TopologyBuilder topologyBuilder = TopologyUtil.buildTopology(topologyName, conf);
            JStormHelper.runTopology(topologyBuilder.createTopology(), topologyName, conf, 60,
                    new JStormHelper.CheckAckedFail(conf), isLocal);
        } catch (Exception e) {
            logger.error("MyMainTopology main Exception",  e);
            throw new RuntimeException(e);
        }
    }


}
