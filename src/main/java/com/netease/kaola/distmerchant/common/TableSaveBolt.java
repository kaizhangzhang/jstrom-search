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
 * @description db保存，如果sqlcheck存在值则更新，否则insert
 * @since 2019/1/31
 */
public class TableSaveBolt extends BaseBasicBolt {
    private static final Logger logger = LoggerFactory.getLogger(TableSaveBolt.class);
    private BoltXmlBean boltXmlBean;
    private Map<String, String> dbConf = new HashMap<>();

    public TableSaveBolt(BoltXmlBean boltXmlBean) {
        this.boltXmlBean = boltXmlBean;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        dbConf = (Map<String, String>) stormConf.get(boltXmlBean.getDatasource());
    }

    public boolean executeSql(List<Object> queryParams, List<Object> insertOrUpdateValues) {
        Connection connection = null;
        try {
            connection = initJdbcConnection();
            //先自行查询sql，存在则更新否则插入
            if (StringUtils.isNotBlank(boltXmlBean.getQuery())) {
                List<Map<String, Object>> queryResult = JdbcUtil.querySql(connection, boltXmlBean.getQuery(), queryParams);
                if (CollectionUtils.isNotEmpty(queryResult)) {
                    //update
                    return JdbcUtil.executeSql(connection, boltXmlBean.getUpdate(), insertOrUpdateValues);
                } else {
                    //insert
                    return JdbcUtil.executeSql(connection, boltXmlBean.getInsert(), insertOrUpdateValues);
                }
            } else if (StringUtils.isNotBlank(boltXmlBean.getInsert())) {
                //insert
                return JdbcUtil.executeSql(connection, boltXmlBean.getInsert(), insertOrUpdateValues);
            }
        } catch (Exception e) {
            logger.error(LogInfoFormat.formatBoltLog(boltXmlBean.getId()) + "executeSql Exception", e);
            throw new FailedException(LogInfoFormat.formatBoltLog(boltXmlBean.getId()) + "executeSql Exception", e);
        } finally {
            try {
                JdbcUtil.closeResource(connection, null, null);
            } catch (SQLException e) {
                logger.error(LogInfoFormat.formatBoltLog(boltXmlBean.getId()) + "executeSql close connection Exception", e);
                throw new FailedException(LogInfoFormat.formatBoltLog(boltXmlBean.getId()) + "executeSql close connection Exception", e);
            }
        }
        return false;
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
            Map<String, Object> allReciveMap = BoltUtil.tuple2Map(input);
            List<String> queryConditions = this.buildQuerySqlCondition(input);
            //查询参数
            List<Object> queryParams = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(queryConditions)) {
                for (String condition : queryConditions) {
                    //不做判空，因为占位符?
                    Object obj = allReciveMap.get(condition);
                    queryParams.add(obj);
                }
            }
            //插入或者更新参数
            List<Object> insertOrUpdateParams = new ArrayList<>();
            if (StringUtils.isNotBlank(boltXmlBean.getOutputcolums())) {
                List<String> outputColumns = BoltUtil.parseOutputcolumsFields(boltXmlBean.getOutputcolums());
                if (CollectionUtils.isNotEmpty(outputColumns)) {
                    for (String col : outputColumns) {
                        insertOrUpdateParams.add(allReciveMap.get(col));
                    }
                }
            }
            this.executeSql(queryParams, insertOrUpdateParams);
            if (StringUtils.isNotBlank(boltXmlBean.getOutputfield()) && !allReciveMap.isEmpty()) {
                collector.emit(new Values(allReciveMap));
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

    private Connection initJdbcConnection() throws Exception {
        Connection connection = JdbcUtil.getConnection(dbConf.get(Constants.DB_DRIVER), dbConf.get(Constants.DB_URL),
                dbConf.get(Constants.DB_USER_NAME), dbConf.get(Constants.DB_PASS_WORD));
        return connection;
    }
}
