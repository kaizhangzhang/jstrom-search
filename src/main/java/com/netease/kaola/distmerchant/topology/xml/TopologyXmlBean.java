package com.netease.kaola.distmerchant.topology.xml;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * @author kai.zhang
 * @description TODO
 * @since 2019/1/31
 */
@XmlRootElement(name = "topology")
public class TopologyXmlBean {
    private List<BoltsXmlBean> bolts;

    public List<BoltsXmlBean> getBolts() {
        return bolts;
    }

    public void setBolts(List<BoltsXmlBean> bolts) {
        this.bolts = bolts;
    }
}
