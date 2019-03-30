package com.netease.kaola.distmerchant.yaml;

import org.junit.Test;
import org.springframework.beans.BeanUtils;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.*;
import java.net.URL;
import java.util.Enumeration;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

/**
 * @author kai.zhang
 * @description TODO
 * @since 2019/1/31
 */
public class YamlTest {
    @Test
    public void loadYamlTest() throws IOException {

//        InputStream in = this.getClass().getResourceAsStream("/datasource.yaml");
//        URL resource = YamlTest.class.getClass().getResource("/");
//        URL resources = Thread.currentThread().getContextClassLoader().getResource("/");
//
////        System.out.println(this.getClass().getResource());
//        Yaml yaml = new Yaml(new SafeConstructor());
//        Map ret = (Map) yaml.load(new InputStreamReader(in));
//        System.out.println(ret);

        System.out.println(YamlTest.class.getProtectionDomain().getCodeSource().getLocation().getPath());

        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] resources1 = resolver.getResources("classpath:*.yaml");
        for (Resource r : resources1) {
            System.out.println(r.getFilename());
        }
    }

    @Test
    public void zipTest() {
        String jarFile = "D:\\Users\\kai.zhang\\kaola-project\\dist-merchant-search\\target\\dist-merchant-search-0.0.1-SNAPSHOT.jar";
        try {
            ZipFile zipFile = new ZipFile(jarFile);
            ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(jarFile));
            ZipEntry entry = null;
            while ((entry = zipInputStream.getNextEntry()) != null) {
                if (!entry.isDirectory() && entry.getName().indexOf("/") <= 0) {
                    System.out.println(entry.getName());

                }
            }
        } catch (IOException e) {
        }

    }
}
