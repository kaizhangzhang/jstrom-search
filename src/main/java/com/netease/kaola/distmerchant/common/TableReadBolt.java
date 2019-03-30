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
import com.netease.kaola.distmerchant.common.util.LogInfoFormat;
import com.netease.kaola.distmerchant.common.util.db.JdbcUtil;
import com.netease.kaola.distmerchant.topology.xml.BoltXmlBean;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;


/**
 * @author kai.zhang
 * @description db处理bolt
 * @since 2019/1/30
 */
public class TableReadBolt extends BaseBasicBolt {
    private static final Logger logger = LoggerFactory.getLogger(TableReadBolt.class);
    private BoltXmlBean boltXmlBean;
    private Map<String, String> dbConf = new HashMap<>();

    public TableReadBolt(BoltXmlBean boltXmlBean) {
        this.boltXmlBean = boltXmlBean;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        dbConf = (Map<String, String>) stormConf.get(boltXmlBean.getDatasource());
    }

    public List<Map<String, Object>> executeQuerySql(List<Object> params) {
        List<Map<String, Object>> mapList = new ArrayList<>();
        Connection connection = null;
        try {
            connection = initJdbcConnection();
            mapList = JdbcUtil.querySql(connection, boltXmlBean.getQuery(), params);
        } catch (Exception e) {
            logger.error(LogInfoFormat.formatBoltLog(boltXmlBean.getId()), e);
            throw new FailedException(LogInfoFormat.formatBoltLog(boltXmlBean.getId()), e);
        } finally {
            try {
                JdbcUtil.closeResource(connection, null,null);
            } catch (SQLException e) {
                logger.error(LogInfoFormat.formatBoltLog(boltXmlBean.getId()), e);
                throw new FailedException(LogInfoFormat.formatBoltLog(boltXmlBean.getId()) + "colse db resource Exception", e);
            }
        }
        return mapList;
    }

    /**
     * 查询sql默认根据上一个节点outputField查询，可覆盖此方法修改查询条件
     * @return
     */
    public List<String> buildQuerySqlCondition(Tuple input) {
        List<String> queryList = new ArrayList<>();
        if (input.getFields() != null && input.getFields().size() > 0) {
            queryList.addAll(input.getFields().toList());
        }
        return queryList;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            Map<String, Object> allEmitData = BoltUtil.tuple2Map(input);
            List<String> queryConditions = this.buildQuerySqlCondition(input);
            List<Object> queryParams = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(queryConditions)) {
                for (String condition : queryConditions) {
                    Object obj = allEmitData.get(condition);
                    //不做判空，因为占位符?
                    queryParams.add(obj);
                }
            }
            List<Map<String, Object>> maps = executeQuerySql(queryParams);
            if (StringUtils.isNotBlank(boltXmlBean.getOutputfield())) {
                if (CollectionUtils.isNotEmpty(maps)) {
                    for (Map<String, Object> m : maps) {
                        m.putAll(allEmitData);
                        collector.emit(new Values(m));
                    }
                } else {
                    collector.emit(new Values(allEmitData));
                }
            }
        } catch (Exception e) {
            logger.error(LogInfoFormat.formatBoltLog(boltXmlBean.getId()), e);
            throw new FailedException(LogInfoFormat.formatBoltLog(boltXmlBean.getId()), e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (StringUtils.isNotBlank(boltXmlBean.getOutputfield())) {
            declarer.declare(new Fields(boltXmlBean.getOutputfield()));
        }
    }

    private Connection initJdbcConnection() throws Exception {
        Connection connection = JdbcUtil.getConnection(dbConf.get(Constants.DB_DRIVER), dbConf.get(Constants.DB_URL),
                dbConf.get(Constants.DB_USER_NAME), dbConf.get(Constants.DB_PASS_WORD));
        return connection;
    }
}
