package com.netease.kaola.distmerchant.common.util.db;

import org.apache.hadoop.hbase.client.Result;

/**
 * @author kai.zhang
 * @since 2019/3/4
 */
public abstract class HbaseResultCallback<T> {
    public abstract T parseResult(Result result);
}
