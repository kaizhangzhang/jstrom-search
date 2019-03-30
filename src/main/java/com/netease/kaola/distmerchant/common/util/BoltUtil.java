package com.netease.kaola.distmerchant.common.util;

import backtype.storm.tuple.Tuple;
import com.netease.kaola.distmerchant.common.Constants;
import org.apache.commons.lang.StringUtils;

import java.util.*;

/**
 * @author kai.zhang
 * @since 2019/2/13
 */
public class BoltUtil {

    public static Map<String, Object> tuple2Map(Tuple input) {
        Map<String, Object> mapResult = new HashMap<>();
        if (input == null) {
            return mapResult;
        }
        if (input.getFields() != null && input.getFields().size() > 0) {
            List<String> fieldKeys = input.getFields().toList();
            for (String field : fieldKeys) {
                Object value = input.getValue(input.getFields().fieldIndex(field));
                if (value instanceof Map) {
                    mapResult.putAll((Map<? extends String, ?>) value);
                } else {
                    mapResult.put(field, value);
                }

            }
        }
        return mapResult;
    }

    public static Map<String, String> parseOutputcolumsFieldsWithDefaultValue(String outputColums) {
        if (StringUtils.isBlank(outputColums)) {
            return Collections.emptyMap();
        }
        Map<String, String> fieldMap = new HashMap<>();
        String[] fields = outputColums.trim().split(Constants.OUTPUT_COLUMNS_SPLIT);
        if (fields != null && fields.length > 0) {
            for (String field : fields) {
                String[] fieldAndDefaultValues = field.trim().split(Constants.OUTPUT_COLUMNS_DEFAULT_VALUE_SPLIT);
                if (fieldAndDefaultValues.length == 1) {
                    fieldMap.put(fieldAndDefaultValues[0].trim(), null);
                } else {
                    fieldMap.put(fieldAndDefaultValues[0].trim(), fieldAndDefaultValues[1].trim());
                }
            }
        }
        return fieldMap;
    }

    public static List<String> parseOutputcolumsFields(String outputColums) {
        if (StringUtils.isBlank(outputColums)) {
            return Collections.emptyList();
        }
        List<String> resultList = new ArrayList<>();
        String[] fields = outputColums.trim().split(Constants.OUTPUT_COLUMNS_SPLIT);
        if (fields != null && fields.length > 0) {
            for (String field : fields) {
                if (StringUtils.isNotBlank(field)) {
                    resultList.add(field.trim());
                }
            }
        }
        return resultList;
    }
    public static void main(String... args) {
        String output = "order_type:1, commission_amount, buy_account:kai.zhang , gorder_benefit_user, pay_amount:100, pay_success_time: , gorder_id, order_id";
        List<String> map = parseOutputcolumsFields(output);
        System.out.println(map);
    }
}
