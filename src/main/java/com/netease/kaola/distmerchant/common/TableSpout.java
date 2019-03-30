package com.netease.kaola.distmerchant.common;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.ReportedFailedException;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.netease.haitao.solo.MultiSoloManager;
import com.netease.haitao.solo.SoloKey;
import com.netease.kaola.distmerchant.common.util.LogInfoFormat;
import com.netease.kaola.distmerchant.common.util.db.JdbcUtil;
import com.netease.kaola.distmerchant.common.util.solo.SoloClientFactory;
import com.netease.kaola.distmerchant.topology.xml.SpoutXmlBean;
import com.netease.kaola.market.common.service.solo.impl.SoloServiceImpl;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author kai.zhang
 * @description db spout
 * @since 2019/1/30
 */
public class TableSpout extends BaseRichSpout {
    private static final Logger logger = LoggerFactory.getLogger(TableSpout.class);
    private static final int LOCK_TIME = 60;
    private SpoutOutputCollector collector;
    private SpoutXmlBean spoutXmlBean;
    private Map<String, String> dbConf = new HashMap<>();
    private SoloServiceImpl soloService;
    private SoloKey soloKey;
    public TableSpout(SpoutXmlBean spoutXmlBean) {
        this.spoutXmlBean = spoutXmlBean;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        dbConf = (Map<String, String>) conf.get(spoutXmlBean.getDatasource());
        Map<String, Object> soloConfig = (Map<String, Object>) conf.get(Constants.DIST_SOLO_MDB_CACHE_CLIENT);
        Integer soloNamespace = soloConfig.get(Constants.SOLO_NAMESPACE) == null ? null : Integer.valueOf(soloConfig.get(Constants.SOLO_NAMESPACE).toString());
        try {
            MultiSoloManager distComposeSoloInstance = SoloClientFactory.getDistComposeSoloInstance(soloConfig);
            soloService = new SoloServiceImpl();
            soloService.setSoloClient(distComposeSoloInstance);
            soloService.setNamespace(soloNamespace);
            soloKey = new SoloKey();
            soloKey.setKey(spoutXmlBean.getQuery());
        } catch (Exception e) {
            logger.error(LogInfoFormat.formatSpoutLog(spoutXmlBean.getId()) + " open Exception", e);
            throw new ReportedFailedException(e);
        }
    }

    /**
     *
     * @return
     */
    public List<Map<String, Object>> getSpoutData() {
        List<Map<String, Object>> dbResult = new ArrayList<>();
        List<Object> deleteList = new ArrayList<>();
        Connection connection = null;
        try {
            boolean lock = soloService.getLock(soloKey, LOCK_TIME, TimeUnit.SECONDS);
            if (!lock) {
                return Collections.emptyList();
            }
            connection = initJdbcConnection();
            dbResult = JdbcUtil.querySql(connection, spoutXmlBean.getQuery(), null);
            if (CollectionUtils.isNotEmpty(dbResult)) {
                for (Map<String, Object> m : dbResult) {
                    Object key = m.get(Constants.SPOUT_TABLE_DELETE_KEY);
                    //必须保证emit出去的值是被同一个bolt处理，那么value必须相同
                    // @see MkFieldsGrouper.batchGrouper
                    m.remove(Constants.SPOUT_TABLE_DELETE_KEY);
                    if (key != null) {
                        deleteList.add(key);
                    }
                }
            }
            if (StringUtils.isNotBlank(spoutXmlBean.getDelete()) && CollectionUtils.isNotEmpty(deleteList)) {
                //TODO 后续优化，可优化成批量删除
                for (Object deleteId : deleteList) {
                    JdbcUtil.executeSql(connection, spoutXmlBean.getDelete(), Collections.singletonList(deleteId));
                }
            }
        } catch (Exception e) {
            logger.error(LogInfoFormat.formatSpoutLog(spoutXmlBean.getId()), e);
            throw new FailedException(LogInfoFormat.formatSpoutLog(spoutXmlBean.getId()) + "getSpoutData Exception", e);
        } finally {
            try {
                JdbcUtil.closeResource(connection, null, null);
                soloService.releaseLock(soloKey);
            } catch (Exception e) {
                logger.error(LogInfoFormat.formatSpoutLog(spoutXmlBean.getId()) + "closeResource Exception: ", e);
//                throw new FailedException(LogInfoFormat.formatSpoutLog(spoutXmlBean.getId()) + "closeResource Exception: ", e);
            }
        }
        return dbResult;
    }

    @Override
    public void nextTuple() {
        //发送到下一个bolt
        try {
            List<Map<String, Object>> spoutData = this.getSpoutData();
            //每秒读取
            Thread.sleep(1000);
            if (StringUtils.isNotBlank(spoutXmlBean.getOutputfield())
                    && CollectionUtils.isNotEmpty(spoutData)) {
                for (Map<String, Object> map : spoutData) {
                    collector.emit(new Values(map), map.get(spoutXmlBean.getOutputfield()));
                }
            }
        } catch (Exception e) {
            logger.error(LogInfoFormat.formatSpoutLog(spoutXmlBean.getId()) + " Exception: ", e);
            throw new FailedException(LogInfoFormat.formatSpoutLog(spoutXmlBean.getId()) + " Exception: ", e);
        }
    }

    @Override
    public void ack(Object msgId) {
        logger.info(LogInfoFormat.formatSpoutLog(spoutXmlBean.getId()) + "msgId = " + msgId + " success");
    }

    @Override
    public void fail(Object msgId) {
        logger.error(LogInfoFormat.formatSpoutLog(spoutXmlBean.getId()) + "fail Exception msgId = " + msgId);
        dealIncrMsgId(msgId, spoutXmlBean.getInsert());
    }



    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (StringUtils.isNotBlank(spoutXmlBean.getOutputfield())) {
            declarer.declare(new Fields(spoutXmlBean.getOutputfield()));
        }
    }

    private void dealIncrMsgId(Object msgId, String sql) {
        //插入增量表
        Connection connection = null;
        try {
            connection = initJdbcConnection();
            List<Object> params = new ArrayList<>();
            params.add(msgId);
            JdbcUtil.executeSql(connection, sql, params);
        } catch (Exception e) {
            logger.error(LogInfoFormat.formatSpoutLog(spoutXmlBean.getId()) + "dealIncrMsgId Exception msgId = " + msgId + " sql = " + sql, e);
            throw new FailedException(LogInfoFormat.formatSpoutLog(spoutXmlBean.getId()) + " dealIncrMsgId Exception msgId = " + msgId, e);
        } finally {
            try {
                JdbcUtil.closeResource(connection, null, null);
            } catch (SQLException e) {
                logger.error(LogInfoFormat.formatSpoutLog(spoutXmlBean.getId()) + "Exception msgId = " + msgId + " sql = " + sql, e);
                connection = null;
            }
        }
    }

    private Connection initJdbcConnection() throws Exception {
        Connection connection = JdbcUtil.getConnection(dbConf.get(Constants.DB_DRIVER), dbConf.get(Constants.DB_URL),
                dbConf.get(Constants.DB_USER_NAME), dbConf.get(Constants.DB_PASS_WORD));
        return connection;
    }
}
