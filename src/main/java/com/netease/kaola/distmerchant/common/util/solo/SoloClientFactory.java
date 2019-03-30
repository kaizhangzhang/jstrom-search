package com.netease.kaola.distmerchant.common.util.solo;

import com.baidu.disconf.client.service.ConfigService;
import com.baidu.disconf.client.service.factory.ConfigServiceFactory;
import com.netease.haitao.solo.DisconfCallback;
import com.netease.haitao.solo.MultiSoloManager;
import com.netease.haitao.solo.SoloKey;
import com.netease.haitao.solo.impl.DefaultDisconfCallback;
import com.netease.haitao.solo.impl.DefaultNamespaceConfig;
import com.netease.haitao.solo.impl.MultiSoloManagerImpl;
import com.netease.haitao.solo.monitor.SentrySoloMonitor;
import com.netease.kaola.distmerchant.common.Constants;
import com.netease.kaola.market.common.service.solo.impl.SoloServiceImpl;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author kai.zhang
 * @description solo client factory
 * @since 2019/2/28
 */
public class SoloClientFactory {
    private static final ConcurrentMap<String, MultiSoloManager> soloServiceMap = new ConcurrentHashMap<>();
    public static String buildCacheKey(String soloClusterName,
                                       int namespace) {
        String cacheKey = String.format("%s-%s", soloClusterName, namespace);
        return cacheKey;
    }

    /**
     *
     * @param soloClusterName
     * @param namespace
     * @param environmentName
     * @param isTestEnvironment
     * @param isTestIsolation
     * @param expireSeconds 缓存过期时间，默认是一天
     * @return
     * @throws Exception
     */
    public static MultiSoloManager getSoloManager(
                                            String soloClusterName,
                                            int namespace,
                                            String environmentName,
                                            boolean isTestEnvironment,
                                            boolean isTestIsolation,
                                            Integer expireSeconds
                                                 ) throws Exception {
        String cacheKey = buildCacheKey(soloClusterName, namespace);
        MultiSoloManager soloManager = soloServiceMap.computeIfAbsent(cacheKey, (key) -> {
            try {
                ConfigServiceFactory configServiceFactory = new ConfigServiceFactory();
                configServiceFactory.init();

                DisconfCallback soloConfigCallBack = new DefaultDisconfCallback();
                ConfigService soloClusterService = configServiceFactory.getInstance("solo_cluster", soloConfigCallBack);

                SentrySoloMonitor soloMonitor = new SentrySoloMonitor();

                DefaultNamespaceConfig defaultNamespaceConfig = new DefaultNamespaceConfig();
                defaultNamespaceConfig.setNamespace(namespace);
                defaultNamespaceConfig.setEnvironmentName(environmentName);
                defaultNamespaceConfig.setIsTestEnvironment(isTestEnvironment);
                defaultNamespaceConfig.setUseIsolation(isTestIsolation);
                if (expireSeconds != null) {
                    defaultNamespaceConfig.setExpireSeconds(expireSeconds);
                }

                MultiSoloManager soloClient = new MultiSoloManagerImpl();
                soloClient.setClusterKey(soloClusterName);
                soloClient.setSoloConfigServer(soloClusterService);
                soloClient.setDisconfCallback(soloConfigCallBack);
                soloClient.setNamespaceConfigs(Collections.singleton(defaultNamespaceConfig));
                soloClient.setSoloMonitor(soloMonitor);
                soloClient.init();

                return soloClient;
            } catch (Exception e) {
                throw new RuntimeException("init solo service exception ", e);
            }
        });
        return soloManager;
    }

    public static void close(MultiSoloManager soloManager,  String soloClusterName,
                             int namespace) {
        if (soloManager != null) {
            soloServiceMap.remove(buildCacheKey(soloClusterName, namespace));
            soloManager.close();
        }
    }

    public static MultiSoloManager getDistComposeSoloInstance(Map<String, Object> soloConfig) throws Exception {
        String soloClusterName = (String) soloConfig.get(Constants.SOLO_CLUSTER_NAME);
        Integer soloNameSpace =  soloConfig.get(Constants.SOLO_NAMESPACE) == null ? null : Integer.valueOf(soloConfig.get(Constants.SOLO_NAMESPACE).toString());
        String evnName = (String) soloConfig.get(Constants.ENVIRONMENT_NAME);
        boolean isTestEnv = (boolean) soloConfig.get(Constants.IS_TEST_ENVIRONMENT);
        boolean isIsolateTest = (boolean) soloConfig.get(Constants.IS_ISOLATE_TEST_CACHE);
        Integer putTimeOut = soloConfig.get(Constants.SOLO_DEFAULT_PUT_TIMEOUT) == null ? null : Integer.valueOf(soloConfig.get(Constants.SOLO_DEFAULT_PUT_TIMEOUT).toString());
        MultiSoloManager soloManager = SoloClientFactory.getSoloManager(soloClusterName,
                soloNameSpace,
                evnName,
                isTestEnv,
                isIsolateTest,
                putTimeOut);
        return soloManager;
    }

    public static void main(String... args) {
        try {
            MultiSoloManager soloManager = SoloClientFactory.getSoloManager("solo_cluster_market_thirdpart",
                    401, "test", true, false, null);
            SoloServiceImpl soloService = new SoloServiceImpl();
            soloService.setNamespace(401);
            soloService.setSoloClient(soloManager);
            SoloKey.SoloKeyPrefix<Object> soloKeyPrefix = SoloKey.SoloKeyPrefix.buildKeyPrefix("DIST_JSTORM_TEST");
            soloKeyPrefix.setExpire(1000);
            SoloKey soloKey = soloKeyPrefix.createKey("test");
            soloService.put(soloKey, 12345);

            Object value = soloService.get(soloKey);
            System.out.println(value);
            close(soloManager, "solo_cluster_market_thirdpart", 401);
            value = soloService.get(soloKey);
            System.out.println(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
