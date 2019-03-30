package com.netease.kaola.distmerchant.common.util;

import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.RegistryConfig;
import com.alibaba.dubbo.config.utils.ReferenceConfigCache;
import com.alibaba.dubbo.rpc.service.GenericService;
import com.google.common.base.Preconditions;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.common.cache.CacheBuilder;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @Date: 2018/11/9 15:13
 * @Description: dubbo 泛化调用封装
 */
public class DubboGenericFactory<T> {

    private static final ApplicationConfig applicationConfig = new ApplicationConfig();

    static {
        // 当前应用配置
        applicationConfig.setName("dist-jstrom");
        applicationConfig.setOwner("kai.zhang");
    }

//    public static Object $invoke(String zookeeperAddress, String interfaceName, String interfaceVersion, String group, Integer timeout,
//                                 String methodName, String[] parameterTypes, Object[] parameterValues) {
//        GenericService genericService = get(zookeeperAddress, interfaceName, interfaceVersion, group, timeout);
//        return genericService.$invoke(methodName, parameterTypes, parameterValues);
//    }

    public static <T> T get(String zookeeperAddress, Class<T> t, String interfaceVersion, String group, Integer timeout) {
        boolean b = StringUtils.isBlank(zookeeperAddress);
        Preconditions.checkArgument(!b);

        ReferenceConfig<T> referenceConfig = ReferenceConfigFactory.get(t, interfaceVersion, group, timeout, zookeeperAddress);
        return GenericServiceFactory.get(referenceConfig);
    }


    static class GenericServiceFactory {

        public static <T> T get(ReferenceConfig<T> reference) {
            ReferenceConfigCache configCache = ReferenceConfigCache.getCache();
            return configCache.get(reference);
        }
    }

    static class RegistryConfigFactory {
        private static ConcurrentMap<String, RegistryConfig> zkAddressMap = new ConcurrentHashMap<>();

        public static RegistryConfig get(String zookeeperAddress) {
            if (StringUtils.isBlank(zookeeperAddress)) {
                return null;
            }

            RegistryConfig registryConfig = zkAddressMap.get(zookeeperAddress);

            if (registryConfig == null) {
                RegistryConfig registry = new RegistryConfig();
                registry.setAddress(zookeeperAddress);
                registry.setProtocol("zookeeper");
                registry.setCheck(false);
                registry.setClient("cached");
                registry.setTimeout(30000);
                zkAddressMap.putIfAbsent(zookeeperAddress, registry);
            }

            return zkAddressMap.get(zookeeperAddress);
        }
    }

    static class ReferenceConfigFactory {
        private static ConcurrentMap<String, ReferenceConfig> referenceConfigMap = new ConcurrentHashMap<>();

        public static <T> ReferenceConfig<T> get(Class<T> interfaceName, String interfaceVersion, String group, Integer timeout, String zookeeperAddress) {
            String cacheKey = String.format("%s%s%s", interfaceName.getName()
                    , (StringUtils.isEmpty(interfaceVersion) ? "" : interfaceVersion)
                    , (StringUtils.isEmpty(group) ? "" : group));
            ReferenceConfig referenceConfig = referenceConfigMap.computeIfAbsent(cacheKey, (key) -> {
                // 引用远程服务
                // 该实例很重量，里面封装了所有与注册中心及服务提供方连接，请缓存
                ReferenceConfig<GenericService> reference = new ReferenceConfig<>();
                reference.setApplication(applicationConfig);

                RegistryConfig registryConfig = RegistryConfigFactory.get(zookeeperAddress);
                if (registryConfig != null) {
                    reference.setRegistry(registryConfig);
                }
                if (timeout != null) {
                    reference.setTimeout(timeout);
                } else {
                    reference.setTimeout(5000);
                }
                reference.setCheck(false);

                reference.setGeneric(false);
                if (StringUtils.isNotBlank(interfaceVersion)) {
                    reference.setVersion(interfaceVersion);
                }
                if (StringUtils.isNotBlank(group)) {
                    reference.setGroup(group);
                }

                // 弱类型接口名
                reference.setInterface(interfaceName);
                reference.setId(interfaceName.getName());
                reference.setProtocol("dubbo");
                reference.setRetries(0);
                return reference;
            });


            return referenceConfig;
        }
    }
}
