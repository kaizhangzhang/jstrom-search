package com.netease.kaola.distmerchant.order;

import com.netease.kaola.distmerchant.common.DubboBolt;
import com.netease.kaola.distmerchant.common.util.LogInfoFormat;
import com.netease.kaola.distmerchant.topology.xml.BoltXmlBean;
import com.netease.kaola.entity.model.refund.RefundInfo;
import com.netease.kaola.facade.AftersaleInfoFacade;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author kai.zhang
 * @description item查询售后
 * @since 2019/2/19
 */
public class RefundItemDubboBolt extends DubboBolt<AftersaleInfoFacade> {
    private static final Logger logger = LoggerFactory.getLogger(RefundItemDubboBolt.class);
    public static final String EMIT_ITEM_HAS_REFUND = "has_refund";
    public static final String EMIT_ITEM_REFUND_APPLY_TYPE = "refund_apply_type";
    public static final String EMIT_ITEM_REFUND_APPLY_TIME = "refund_apply_time";


    public RefundItemDubboBolt(BoltXmlBean boltXmlBean) {
        super(boltXmlBean);
    }

    @Override
    public List<Map<String, Object>> invokeDubboService(AftersaleInfoFacade dubboServiceInterface, Map<String, Object> allReciveEmitData) {
        List<Map<String, Object>> result = new ArrayList<>();
        String gorderId = (String) allReciveEmitData.get(TraderGorderDubboBolt.GORDER_ID);
        if (StringUtils.isBlank(gorderId)) {
            return result;
        }
        List<Map<String, Object>> orderList = (List<Map<String, Object>>) allReciveEmitData.get(gorderId);
        if (CollectionUtils.isEmpty(orderList)) {
            return result;
        }
        try {
            Set<String> orderIds = new HashSet<>();
            for (Map<String, Object> map : orderList) {
                String orderId = (String) map.get(TraderGorderDubboBolt.EMIT_ORDER_ID);
                if (StringUtils.isNotBlank(orderId)) {
                    orderIds.add(orderId);
                }
            }
            Map<String, List<RefundInfo>> refundInfoMap = dubboServiceInterface.listRefundInfoByOrderIds(new ArrayList<String>(orderIds));
            if (refundInfoMap != null && !refundInfoMap.isEmpty()) {
                for (Map.Entry<String, List<RefundInfo>> map : refundInfoMap.entrySet()) {
                    List<RefundInfo> refundInfoList = map.getValue();
                    if (CollectionUtils.isNotEmpty(refundInfoList)) {
                        Map<String, RefundInfo> itemRefundInfo = refundInfoList.stream()
                                .collect(Collectors.toMap(refund -> refund.getOrderItemId(), refund -> refund));
                        for (Map<String, Object> orderMap : orderList) {
                            List<Map<String, Object>> itemMapList = (List<Map<String, Object>>) orderMap.get(TraderGorderDubboBolt.EMIT_ITEMS);
                            if (CollectionUtils.isNotEmpty(itemMapList)) {
                                itemMapList.stream().forEach(itemMap -> {
                                    RefundInfo refundInfo = itemRefundInfo.get(itemMap.get(TraderGorderDubboBolt.EMIT_ITEM_ID));
                                    if (refundInfo != null) {
                                        itemMap.put(EMIT_ITEM_HAS_REFUND, true);
                                        itemMap.put(EMIT_ITEM_REFUND_APPLY_TYPE, refundInfo.getApplyType());
                                        itemMap.put(EMIT_ITEM_REFUND_APPLY_TIME, refundInfo.getCreateTime().getTime());
                                    }
                                });
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error(LogInfoFormat.formatBoltLog(boltXmlBean.getId()) + "invokeDubboService AftersaleInfoFacade Exception gorderId = " + gorderId);
            throw new RuntimeException("invokeDubboService AftersaleInfoFacade Exception gorderId = " + gorderId, e);
        }
        return result;
    }
}
