package com.netease.kaola.distmerchant.topology.xml;

import java.util.List;

/**
 * @author kai.zhang
 * @description TODO
 * @since 2019/1/31
 */
public class BoltsXmlBean {
    private List<SpoutXmlBean> spout;
    private List<BoltXmlBean> bolt;

    public List<SpoutXmlBean> getSpout() {
        return spout;
    }

    public void setSpout(List<SpoutXmlBean> spout) {
        this.spout = spout;
    }

    public List<BoltXmlBean> getBolt() {
        return bolt;
    }

    public void setBolt(List<BoltXmlBean> bolt) {
        this.bolt = bolt;
    }
}
