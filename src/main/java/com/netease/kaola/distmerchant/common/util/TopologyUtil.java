package com.netease.kaola.distmerchant.common.util;

import backtype.storm.Config;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import com.netease.kaola.distmerchant.common.Constants;
import com.netease.kaola.distmerchant.topology.xml.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.lang.reflect.Constructor;
import java.util.*;

/**
 * @author kai.zhang
 * @description topology xml解析
 * @since 2019/1/31
 */
public class TopologyUtil {
    private static final Logger logger = LoggerFactory.getLogger(TopologyUtil.class);
    /**
     * 组装topology
     * @param topologyName
     * @param config
     * @return
     * @throws Exception
     */
    public static TopologyBuilder buildTopology(String topologyName, Config config) throws Exception {
        //加载topologyName.xml
        List<LinkedList<BaseXmlBean>> topologyList = parser(topologyName, config);

        if (CollectionUtils.isEmpty(topologyList)) {
            throw new RuntimeException("can not load topology spout and bolt conf from file : " + topologyName + ".xml");
        }
        //bolt并行都默认跟worker数量一致
        Integer paral = (Integer) config.get(Constants.TOPOLOGY_WORKERS);
        TopologyBuilder builder = new TopologyBuilder();
        for (LinkedList<BaseXmlBean> beanLinkedList : topologyList) {
            if (CollectionUtils.isNotEmpty(beanLinkedList)) {
                String preBoltId = "";
                String groupingByPreOutPutField = "";
                for (Iterator<BaseXmlBean> it = beanLinkedList.iterator(); it.hasNext();) {
                    BaseXmlBean baseXmlBean = it.next();
                    if (baseXmlBean instanceof SpoutXmlBean) {
                        //配置spout
                        SpoutXmlBean spout = (SpoutXmlBean) baseXmlBean;
                        //并发数
                        if (spout.getBoltnum() != null && spout.getBoltnum() > 0) {
                            paral = Integer.valueOf(spout.getBoltnum().toString());
                        }
                        //透传配置， spout必须有outputfield
                        if (StringUtils.isBlank(baseXmlBean.getOutputfield())) {
                            throw new RuntimeException("spout outputfield is empty");
                        }
                        Class<?> spoutClass = Class.forName(spout.getClazz());
                        Constructor<?> spoutDeclaredConstructor = spoutClass.getDeclaredConstructor(new Class[]{SpoutXmlBean.class});
                        spoutDeclaredConstructor.setAccessible(true);
                        BaseRichSpout baseRichSpout = (BaseRichSpout) spoutDeclaredConstructor.newInstance(new Object[]{spout});
                        SpoutDeclarer spoutDeclarer = builder.setSpout(spout.getId(), baseRichSpout, paral);
                        //最大pending
                        if (config.get(Constants.TOPOLOGY_MAX_SPOUT_PENDING) != null) {
                            spoutDeclarer.setMaxSpoutPending((Integer) config.get(Constants.TOPOLOGY_MAX_SPOUT_PENDING));
                        }
                        //任务数
                        if (spout.getTasknum() != null) {
                            spoutDeclarer.setNumTasks(spout.getTasknum());
                        }
                        preBoltId = spout.getId();
                        groupingByPreOutPutField = spout.getOutputfield();
                    } else if (baseXmlBean instanceof BoltXmlBean) {
                        //配置bolt
                        BoltXmlBean bolt = (BoltXmlBean) baseXmlBean;
                        //并发数
                        if (bolt.getBoltnum() != null && bolt.getBoltnum() > 0) {
                            paral = Integer.valueOf(bolt.getBoltnum().toString());
                        }
                        Class<?> boltClass = Class.forName(bolt.getClazz());
                        Constructor<?> boltDeclaredConstructor = boltClass.getDeclaredConstructor(new Class[]{BoltXmlBean.class});
                        boltDeclaredConstructor.setAccessible(true);
                        BaseBasicBolt baseRichSpout = (BaseBasicBolt) boltDeclaredConstructor.newInstance(new Object[]{bolt});
                        BoltDeclarer boltDeclarer = builder.setBolt(bolt.getId(), baseRichSpout, paral);

                        if (StringUtils.isNotBlank(bolt.getGrouping())) {
                            if (bolt.getGrouping().equalsIgnoreCase(Constants.SHUFFLE_GROUPING)) {
                                boltDeclarer.localOrShuffleGrouping(preBoltId);
                            } else if (bolt.getGrouping().equalsIgnoreCase(Constants.FIELDS_GROUPING)) {
                                if (StringUtils.isBlank(groupingByPreOutPutField)) {
                                    throw new RuntimeException("bolt set fields grouping, but preOutputField is empty");
                                }
                                boltDeclarer.fieldsGrouping(preBoltId, new Fields(groupingByPreOutPutField));
                            } else if (bolt.getGrouping().equalsIgnoreCase(Constants.ALL_GROUPING)) {
                                boltDeclarer.allGrouping(preBoltId);
                            } else if (bolt.getGrouping().equalsIgnoreCase(Constants.GLOBAL_GROUPING)) {
                                boltDeclarer.globalGrouping(preBoltId);
                            }
                        } else {
                            throw new RuntimeException("bolt grouping is empty");
                        }
                        //任务数
                        if (bolt.getTasknum() != null) {
                            boltDeclarer.setNumTasks(bolt.getTasknum());
                        }
                        preBoltId = bolt.getId();
                        groupingByPreOutPutField = bolt.getOutputfield();
                    }
                }
            }
        }
        return builder;
    }
    /**
     * 解析xml配置，构建拓扑图
     * @param topologyName
     * @param config
     * @return
     * @throws JAXBException
     */
    public static List<LinkedList<BaseXmlBean>> parser(String topologyName, Config config) throws JAXBException {
        if (StringUtils.isBlank(topologyName)) {
            return new ArrayList<>();
        }
        Map<String, SpoutXmlBean> spoutXmlBeanMap = new HashMap<>();
        Map<String, BoltXmlBean> boltXmlBeanMap = new HashMap<>();
        String fileName = topologyName + ".xml";
        JAXBContext jaxbContext = JAXBContext.newInstance(TopologyXmlBean.class);
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
        TopologyXmlBean topologyXmlBean = (TopologyXmlBean) unmarshaller.unmarshal(TopologyUtil.class.getResourceAsStream("/" + fileName));
        //xml配置组装
        if (topologyXmlBean != null && CollectionUtils.isNotEmpty(topologyXmlBean.getBolts())) {
            for (BoltsXmlBean bolts : topologyXmlBean.getBolts()) {
                if (CollectionUtils.isNotEmpty(bolts.getSpout())) {
                    for (SpoutXmlBean spout : bolts.getSpout()) {
                        SpoutXmlBean spoutMapObj = spoutXmlBeanMap.get(spout.getId());
                        if (spoutMapObj != null) {
                            BeanUtils.copyPropertiesIgnoreNull(spout, spoutMapObj);
                        } else {
                            spoutXmlBeanMap.put(spout.getId(), spout);
                        }
                    }
                }
                if (CollectionUtils.isNotEmpty(bolts.getBolt())) {
                    for (BoltXmlBean bolt : bolts.getBolt()) {
                        BoltXmlBean boltMapObj = boltXmlBeanMap.get(bolt.getId());
                        if (boltMapObj != null) {
                            BeanUtils.copyPropertiesIgnoreNull(bolt, boltMapObj);
                        } else {
                            boltXmlBeanMap.put(bolt.getId(), bolt);
                        }
                    }
                }
            }
        }
        if (!spoutXmlBeanMap.isEmpty()) {
            for (Map.Entry<String, SpoutXmlBean> entry : spoutXmlBeanMap.entrySet()) {
                Map<String, Object> stringObjectMap = BeanUtils.beanToMap(entry.getValue());
                config.put(entry.getKey(), stringObjectMap);
            }
        }
        if (!boltXmlBeanMap.isEmpty()) {
            for (Map.Entry<String, BoltXmlBean> entry : boltXmlBeanMap.entrySet()) {
                Map<String, Object> stringObjectMap = BeanUtils.beanToMap(entry.getValue());
                config.put(entry.getKey(), stringObjectMap);
            }
        }
        //根据sput拼成topology链
        List<LinkedList<BaseXmlBean>> topologyMapping = new ArrayList<>();
        for (Map.Entry<String, SpoutXmlBean> entry : spoutXmlBeanMap.entrySet()) {
            LinkedList<BaseXmlBean> spoutLinked = new LinkedList<>();
            BaseXmlBean value = entry.getValue();
            spoutLinked.add(value);
            if (StringUtils.isNotBlank(value.getNextbolt())) {
                BoltXmlBean boltXmlBean = boltXmlBeanMap.get(value.getNextbolt());
                while (boltXmlBean != null) {
                    spoutLinked.add(boltXmlBean);
                    if (StringUtils.isBlank(boltXmlBean.getNextbolt())) {
                        break;
                    }
                    boltXmlBean = boltXmlBeanMap.get(boltXmlBean.getNextbolt());
                    if (boltXmlBean == null) {
                        break;
                    }
                }
            }
            topologyMapping.add(spoutLinked);
        }
        return topologyMapping;
    }
    public static void main(String[] args) throws Exception {
        TopologyUtil.parser("dist-order-search", new Config());
    }
}
