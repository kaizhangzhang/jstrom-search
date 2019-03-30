package com.netease.kaola.distmerchant.common.util;

import com.netease.kaola.distmerchant.common.Constants;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author kai.zhang
 * @description TODO
 * @since 2019/2/21
 */
public class ElasticSearchFactory {
    //TCP三次握手建立链接时间
    private static final int CONNECT_TIME_OUT = 5 * 1000;
    //等待服务端返回响应时间
    private static final int SOCKET_TIME_OUT = 3 * 1000;
    //httpclient线程池获取链接等待时间
    private static final int CONNECTION_REQUEST_TIME_OUT = 1000;
    //httpclient线程池大小
    private static final int MAX_CONNECT_NUM = 100;
    //每个站点对应的连接数
    private static final int MAX_CONNECT_PER_ROUTE = 100;

    private static final ConcurrentMap<String, RestHighLevelClient> restHighLevelClientConcurrentMap = new ConcurrentHashMap<>();

    public static String buildCacheKey(String host, int port, String schema) {
        String cacheKey = String.format("%s-%s-%s", host, port, schema);
        return cacheKey;
    }

    public static RestHighLevelClient getClient(String host, int port, String schema) {
        String cacheKey = buildCacheKey(host, port, schema);
        RestHighLevelClient restHighLevelClient = restHighLevelClientConcurrentMap.computeIfAbsent(cacheKey, (key) -> {
            String[] hosts = host.split(",");
            HttpHost[] httpHosts = new HttpHost[hosts.length];
            for (int i = 0; i < hosts.length; i++) {
                httpHosts[i] = new HttpHost(hosts[i], port, schema);
            }
            RestClientBuilder builder = RestClient.builder(httpHosts);
            // 主要关于异步httpclient的连接延时配置
            builder.setRequestConfigCallback((configBuilder) -> {
                configBuilder.setConnectTimeout(CONNECT_TIME_OUT);
                configBuilder.setSocketTimeout(SOCKET_TIME_OUT);
                configBuilder.setSocketTimeout(CONNECTION_REQUEST_TIME_OUT);
                return configBuilder;
            });
            // 主要关于异步httpclient的连接数配置
            builder.setHttpClientConfigCallback((httpAsyncClientBuilder) -> {
                httpAsyncClientBuilder.setMaxConnTotal(MAX_CONNECT_NUM);
                httpAsyncClientBuilder.setMaxConnPerRoute(MAX_CONNECT_PER_ROUTE);
                return httpAsyncClientBuilder;
            });
            return new RestHighLevelClient(builder);
        });
        return restHighLevelClient;
    }
    public static void close(RestHighLevelClient client, String host, int port, String schema) {
        try {
            if (client != null) {
                restHighLevelClientConcurrentMap.remove(buildCacheKey(host, port, schema));
                client.close();
            }
        } catch (IOException e) {

        }
    }
}
