package com.netease.kaola.distmerchant.order;

import com.netease.kaola.distmerchant.common.DubboBolt;
import com.netease.kaola.distmerchant.common.util.LogInfoFormat;
import com.netease.kaola.distmerchant.topology.xml.BoltXmlBean;
import com.netease.kaola.order.compose.service.address.OrderAddressCompose;
import com.netease.kaola.order.compose.vo.order.OrderAddressStandaloneVO;
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
 * @description 订单收货人地址服务
 * @since 2019/2/19
 */
public class OrderAddressDubboBolt extends DubboBolt<OrderAddressCompose> {
    private static final Logger logger = LoggerFactory.getLogger(OrderAddressDubboBolt.class);

    public static final String EMIT_BUY_ACCOUNT_NAME = "buy_account_name";
    public static final String EMIT_BUY_ACCOUNT_PHONE = "buy_account_phone";

    public OrderAddressDubboBolt(BoltXmlBean boltXmlBean) {
        super(boltXmlBean);
    }

    @Override
    public List<Map<String, Object>> invokeDubboService(OrderAddressCompose dubboServiceInterface, Map<String, Object> allReciveEmitData) {
        List<Map<String, Object>> resultList = new ArrayList<>();
        String gorderId = (String) allReciveEmitData.get(TraderGorderDubboBolt.GORDER_ID);
        if (StringUtils.isBlank(gorderId)) {
            return resultList;
        }
        List<Map<String, Object>> orderMapList = (List<Map<String, Object>>) allReciveEmitData.get(gorderId);
        if (CollectionUtils.isEmpty(orderMapList)) {
            return resultList;
        }
        try {
            Map<String, OrderAddressStandaloneVO> orderAddressMap = dubboServiceInterface.queryOrderAddressVosByGorderId(gorderId);
            if (orderAddressMap != null && !orderAddressMap.isEmpty()) {
                for (Map<String, Object> map : orderMapList) {
                    String orderId = (String) map.get(TraderGorderDubboBolt.EMIT_ORDER_ID);
                    OrderAddressStandaloneVO orderAddressStandaloneVO = orderAddressMap.get(orderId);
                    if (orderAddressStandaloneVO != null) {
                        map.put(EMIT_BUY_ACCOUNT_NAME, orderAddressStandaloneVO.getName());
                        map.put(EMIT_BUY_ACCOUNT_PHONE, orderAddressStandaloneVO.getRecieverPhone());
                    }
                }
            }
        } catch (Exception e) {
            logger.error(LogInfoFormat.formatBoltLog(boltXmlBean.getId()) +
                    "invokeDubboService OrderAddressCompose Exception gorder_id = {}", gorderId, e);
            throw new RuntimeException("invokeDubboService OrderAddressCompose Exception", e);
        }
        return resultList;
    }
}
