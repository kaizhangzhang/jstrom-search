package com.netease.kaola.distmerchant.order;

import com.netease.kaola.distmerchant.common.DubboBolt;
import com.netease.kaola.distmerchant.common.util.LogInfoFormat;
import com.netease.kaola.distmerchant.topology.xml.BoltXmlBean;
import com.netease.kaola.order.compose.service.GorderCompose;
import com.netease.kaola.order.compose.vo.gorder.GorderFullWithOrdersVO;
import com.netease.kaola.order.compose.vo.order.OrderFullWithItemsVO;
import com.netease.kaola.order.compose.vo.order.OrderVO;
import com.netease.kaola.order.compose.vo.orderitem.OrderItemVO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author kai.zhang
 * @since 2019/2/13
 */
public class TraderGorderDubboBolt extends DubboBolt<GorderCompose> {
    private static final Logger logger = LoggerFactory.getLogger(TraderGorderDubboBolt.class);
    public static final String GORDER_ID = "gorder_id";
    public static final String SHOP_ID = "shop_id";
    public static final String EMIT_ORDER_ID = "order_id";
    public static final String EMIT_ORDER_STAYUS= "order_status";
    public static final String EMIT_ORDER_TIME = "order_time";
    public static final String EMIT_ORDER_PAY_AMOUNT = "order_pay_amount";

    public static final String EMIT_ITEMS = "items";
    public static final String EMIT_ITEM_IDEX_ID = "index_id";
    public static final String EMIT_ITEM_ID = "item_id";
    public static final String EMIT_ITEM_GOODS_ID = "goods_id";
    public static final String EMIT_ITEM_GOODS_NAME = "goods_name";
    public static final String EMIT_ITEM_GOODS_IMG = "goods_img";
    public static final String EMIT_ITEM_SKU_ID = "sku_id";
    public static final String EMIT_ITEM_SKU_DESC = "sku_desc";
    public static final String EMIT_ITEM_UNIT_PRICE = "unit_price";
    public static final String EMIT_ITEM_ITEM_COUNT = "item_count";

    public TraderGorderDubboBolt(BoltXmlBean boltXmlBean) {
        super(boltXmlBean);
    }

    @Override
    public List<Map<String, Object>> invokeDubboService(GorderCompose dubboServiceInterface, Map<String, Object> allReciveEmitData) {
        List<Map<String, Object>> resultList = new ArrayList<>();
        String gorderId = (String) allReciveEmitData.get(GORDER_ID);
        String shopId = (String) allReciveEmitData.get(SHOP_ID);
        try {
            GorderFullWithOrdersVO gorderFullWithOrdersVO = dubboServiceInterface.queryGorderInfo(gorderId);
            if (gorderFullWithOrdersVO != null) {
                Map<String, Object> gorderMap = new HashMap<>();
                List<Map<String, Object>> orderMapList = new ArrayList<>();
                for (OrderFullWithItemsVO orderFullWithItemsVO : gorderFullWithOrdersVO.getOrderFullWithItemsVOs()) {
                    OrderVO orderVO = orderFullWithItemsVO.getOrderVO();
                    Map<String, Object> orderMap = new HashMap<>();
                    orderMap.put(GORDER_ID, gorderId);
                    orderMap.put(SHOP_ID, shopId);
                    orderMap.put(EMIT_ORDER_ID, orderVO.getId());
                    orderMap.put(EMIT_ORDER_STAYUS, orderVO.getOrderStatus().intValue());
                    orderMap.put(EMIT_ORDER_TIME, orderVO.getOrderTime().getTime());
                    orderMap.put(EMIT_ORDER_PAY_AMOUNT, orderVO.getPayAmount());
                    List<Map<String, Object>> itemMapList = new ArrayList<>();
                    int indexId = 0;
                    for (OrderItemVO itemVO :  orderFullWithItemsVO.getOrderItemVOs()) {
                        Map<String, Object> itemMap = new HashMap<>();
                        itemMap.put(EMIT_ITEM_IDEX_ID, indexId++);
                        itemMap.put(EMIT_ITEM_ID, itemVO.getId());
                        itemMap.put(EMIT_ITEM_GOODS_ID, itemVO.getGoodsId());
                        itemMap.put(EMIT_ITEM_GOODS_NAME, itemVO.getProductName());
                        itemMap.put(EMIT_ITEM_GOODS_IMG, itemVO.getGoodsImageUrl());
                        itemMap.put(EMIT_ITEM_SKU_ID, itemVO.getSkuId());
                        itemMap.put(EMIT_ITEM_SKU_DESC, itemVO.getSkuDesc());
                        itemMap.put(EMIT_ITEM_UNIT_PRICE, itemVO.getUnitPrice());
                        itemMap.put(EMIT_ITEM_ITEM_COUNT, itemVO.getBuyCount());
                        //是否有售后默认值0
                        itemMap.put(RefundItemDubboBolt.EMIT_ITEM_HAS_REFUND, false);
                        itemMapList.add(itemMap);
                    }
                    orderMap.put(EMIT_ITEMS, itemMapList);
                    orderMapList.add(orderMap);
                }
                gorderMap.put(gorderId, orderMapList);
                resultList.add(gorderMap);

            } else {
                logger.error(LogInfoFormat.formatBoltLog(boltXmlBean.getId()) + "gorderCompose.queryGorderInfo can not find gorderInfo, gorderId = " + gorderId);
                throw new RuntimeException("gorderCompose.queryGorderInfo can not find gorderInfo, gorderId = " + gorderId);
            }
        } catch (Exception e) {
            logger.error(LogInfoFormat.formatBoltLog(boltXmlBean.getId()) + "call gorderCompose.queryGorderInfo Exception gorderId = ", gorderId, e);
            throw new RuntimeException("call gorderCompose.queryGorderInfo Exception gorderId = " + gorderId, e);
        }
        return resultList;
    }


}
