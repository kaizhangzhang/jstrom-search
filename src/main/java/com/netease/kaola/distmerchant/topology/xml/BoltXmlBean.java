package com.netease.kaola.distmerchant.topology.xml;

/**
 * @author kai.zhang
 * @description TODO
 * @since 2019/1/31
 */
public class BoltXmlBean extends BaseXmlBean{
    private String grouping;
    private String outputcolums;
    private String dubbo;

    public String getOutputcolums() {
        return outputcolums;
    }

    public void setOutputcolums(String outputcolums) {
        this.outputcolums = outputcolums;
    }

    public String getGrouping() {
        return grouping;
    }

    public void setGrouping(String grouping) {
        this.grouping = grouping;
    }

    public String getDubbo() {
        return dubbo;
    }

    public void setDubbo(String dubbo) {
        this.dubbo = dubbo;
    }
}
