package com.netease.kaola.distmerchant.common;

/**
 * @author kai.zhang
 * @description 常量
 * @since 2019/1/30
 */
public interface Constants {
    //topology

    String RESOURCE_CONF_PATH = "dist-conf/";
    String TOPOLOGY_NAME = "topology.name";
    String STORM_CLUSTER_MODE = "storm.cluster.mode";
    String TOPOLOGY_WORKERS = "topology.workers";
    String TOPOLOGY_MAX_SPOUT_PENDING = "topology.max.spout.pending";

    /**
     * 轮询方式，平均分配tuple到下一级component上
     */
    String SHUFFLE_GROUPING = "shuffle";
    /**
     * 类似SQL中的group by， 保证相同的Key的数据会发送到相同的task， 原理是 对某个或几个字段做hash，然后用hash结果求模得出目标taskId
     */
    String FIELDS_GROUPING = "fields";
    /**
     * 发送给target component所有task
     */
    String ALL_GROUPING = "all";
    /**
     * target component第一个task
     */
    String GLOBAL_GROUPING = "global";

    //db

    String DB_URL = "url";
    String DB_USER_NAME = "username";
    String DB_PASS_WORD = "password";
    String DB_DRIVER = "driver";

    String OUTPUT_COLUMNS_SPLIT = ",";
    String OUTPUT_COLUMNS_DEFAULT_VALUE_SPLIT = ":";

    //dubbo

    String DUBBO_ZK_ADDRESS = "dubbo.zk.address";
    String DUBBO_CONSUMER = "dubbo.consumer";
    String DUBBO_INTERFACE = "interface";
    String DUBBO_VERSION = "version";
    String DUBBO_GROUP = "group";
    String DUBBO_TIMEOUT = "timeout";

    //spout key

    String SPOUT_TABLE_DELETE_KEY = "incrTableDeleteKey";

    //elastic search
    String ELASTIC_HOST = "host";
    String ELASTIC_PORT = "port";
    String ELASTIC_SCHEMA = "schema";
    String ELASTIC_INDEX = "index";
    String ELASTIC_TYPE = "type";
    String ELASTIC_DOC_ID = "_doc_id";

    //mdb conf

    String SOLO_NAMESPACE = "solo.namespace";
    String SOLO_CLUSTER_NAME = "solo.cluster.name";
    String SOLO_DEFAULT_PUT_TIMEOUT = "solo.default.put.timeout";
    String IS_TEST_ENVIRONMENT = "isTestEnvironment";
    String IS_ISOLATE_TEST_CACHE = "isIsolateTestCache";
    String ENVIRONMENT_NAME = "environment.name";
    String DIST_SOLO_MDB_CACHE_CLIENT = "dist_solo_mdb_cache";
}
