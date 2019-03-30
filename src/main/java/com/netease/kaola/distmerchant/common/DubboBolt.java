package com.netease.kaola.distmerchant.common;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.netease.kaola.distmerchant.common.util.BoltUtil;
import com.netease.kaola.distmerchant.common.util.DubboGenericFactory;
import com.netease.kaola.distmerchant.common.util.LogInfoFormat;
import com.netease.kaola.distmerchant.topology.xml.BoltXmlBean;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author kai.zhang
 * @since 2019/2/13
 */
public abstract class DubboBolt<T> extends BaseBasicBolt {
    private static final Logger logger = LoggerFactory.getLogger(DubboBolt.class);

    protected BoltXmlBean boltXmlBean;
    protected Map<String, Object> dubboConf = new HashMap<>();
    protected String dubboZkAddress;

    public DubboBolt(BoltXmlBean boltXmlBean) {
        this.boltXmlBean = boltXmlBean;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        dubboConf = (Map<String, Object>) ((Map<String, Object>) stormConf.get(Constants.DUBBO_CONSUMER)).get(boltXmlBean.getDubbo());
        dubboZkAddress = (String) stormConf.get(Constants.DUBBO_ZK_ADDRESS);
    }

    /**
     * 调用dubbo，并根据dubbo返回结果构建map, 如果需要回滚，请自行抛异常
     * @param dubboServiceInterface
     * @param allReciveEmitData dubbo 参数
     * @return
     */
    public abstract List<Map<String, Object>> invokeDubboService(T dubboServiceInterface, Map<String, Object> allReciveEmitData);

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            Map<String, Object> allEmitData = BoltUtil.tuple2Map(input);
            //调用dubbo
            T service = DubboGenericFactory.get(dubboZkAddress, (Class<T>) Class.forName((String) dubboConf.get(Constants.DUBBO_INTERFACE)),
                    (String) dubboConf.get(Constants.DUBBO_VERSION), (String) dubboConf.get(Constants.DUBBO_GROUP),
                    dubboConf.get(Constants.DUBBO_TIMEOUT) == null ? null : Integer.valueOf(dubboConf.get(Constants.DUBBO_TIMEOUT).toString()));
            List<Map<String, Object>> dubboResultMap = this.invokeDubboService(service, allEmitData);
            if (StringUtils.isNotBlank(boltXmlBean.getOutputfield())) {
                if (CollectionUtils.isNotEmpty(dubboResultMap)) {
                    for (Map<String, Object> m : dubboResultMap) {
                        m.putAll(allEmitData);
                        collector.emit(new Values(m));
                    }
                } else {
                    collector.emit(new Values(allEmitData));
                }
            }
        } catch (Exception e) {
            logger.error(LogInfoFormat.formatBoltLog(boltXmlBean.getId()) + "Exception", e);
            throw new FailedException(LogInfoFormat.formatBoltLog(boltXmlBean.getId()) + "Exception", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (StringUtils.isNotBlank(boltXmlBean.getOutputfield())) {
            declarer.declare(new Fields(boltXmlBean.getOutputfield()));
        }
    }
}
