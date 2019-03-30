package com.netease.kaola.distmerchant.order;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.FailedException;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.netease.kaola.distmerchant.common.Constants;
import com.netease.kaola.distmerchant.common.ElasticSearchSaveBolt;
import com.netease.kaola.distmerchant.common.util.BoltUtil;
import com.netease.kaola.distmerchant.common.util.LogInfoFormat;
import com.netease.kaola.distmerchant.common.util.PageParam;
import com.netease.kaola.distmerchant.topology.xml.BoltXmlBean;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author kai.zhang
 * @description 订单搜索逻辑
 * @since 2019/2/19
 */
public class DistOrderSearchSaveBolt extends ElasticSearchSaveBolt {
    private static final Logger logger = LoggerFactory.getLogger(DistOrderSearchSaveBolt.class);

    public DistOrderSearchSaveBolt(BoltXmlBean boltXmlBean) {
        super(boltXmlBean);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Map<String, Object> allReciveEmitData = BoltUtil.tuple2Map(input);
        String gorderId = (String) allReciveEmitData.get(TraderGorderDubboBolt.GORDER_ID);
        String shopId = (String) allReciveEmitData.get(TraderGorderDubboBolt.SHOP_ID);
        try {
            List<Map<String, Object>> orderMapList = (List<Map<String, Object>>) allReciveEmitData.get(gorderId);
            if (CollectionUtils.isEmpty(orderMapList)) {
                return;
            }
            Map<String, Map<String, Object>> itemDocIdMap = new HashMap<>();
            for (Map<String, Object> map :  orderMapList) {
                itemDocIdMap.put((String) map.get(TraderGorderDubboBolt.EMIT_ORDER_ID), map);
            }
            //先根据gorderId所以查询, 不存在则插入，存在:状态是待支付删除再插入，支付则更新
            Map<String, Object> searchParams = new HashMap<>();
            searchParams.put(TraderGorderDubboBolt.GORDER_ID, gorderId);
            List<Map<String, Object>> searchResultList = this.querySearch(searchParams, new PageParam(0, PageParam.DEFAULT_PAGE_SIZE), shopId);
            if (CollectionUtils.isNotEmpty(searchResultList)) {
                List<String> deleteDocIdList = new ArrayList<>();
                for (Map<String, Object> searchMap : searchResultList) {
                    String docId = (String) searchMap.get(Constants.ELASTIC_DOC_ID);
                    deleteDocIdList.add(docId);
                }
                this.batchDeteteSerach(deleteDocIdList, shopId);
            }
            this.batchInsertByDocIds(itemDocIdMap, shopId);
            if (StringUtils.isNotBlank(boltXmlBean.getOutputfield())
                    && !allReciveEmitData.isEmpty()) {
                collector.emit(new Values(allReciveEmitData));
            }
        } catch (Exception e) {
            logger.error(LogInfoFormat.formatBoltLog(boltXmlBean.getId()) + "push elastic search gorderId= " + gorderId, e);
            throw new FailedException(LogInfoFormat.formatBoltLog(boltXmlBean.getId()), e);
        }
    }
}
