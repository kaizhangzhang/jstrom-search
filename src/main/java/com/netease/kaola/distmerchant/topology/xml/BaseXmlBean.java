package com.netease.kaola.distmerchant.topology.xml;

import java.io.Serializable;

/**
 * @author kai.zhang
 * @description TODO
 * @since 2019/1/31
 */
public class BaseXmlBean implements Serializable {
    private String id;
    private String clazz;
    private String outputfield;
    private Long boltnum;
    private String nextbolt;
    private Long tasknum;

    //data数据属性
    private String datasource;
    private String query;
    private String insert;
    private String delete;
    private String update;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getClazz() {
        return clazz;
    }

    public void setClazz(String clazz) {
        this.clazz = clazz;
    }

    public String getOutputfield() {
        return outputfield;
    }

    public void setOutputfield(String outputfield) {
        this.outputfield = outputfield;
    }

    public Long getBoltnum() {
        return boltnum;
    }

    public void setBoltnum(Long boltnum) {
        this.boltnum = boltnum;
    }

    public Long getTasknum() {
        return tasknum;
    }

    public void setTasknum(Long tasknum) {
        this.tasknum = tasknum;
    }

    public String getNextbolt() {
        return nextbolt;
    }

    public void setNextbolt(String nextbolt) {
        this.nextbolt = nextbolt;
    }

    public String getDatasource() {
        return datasource;
    }

    public void setDatasource(String datasource) {
        this.datasource = datasource;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getInsert() {
        return insert;
    }

    public void setInsert(String insert) {
        this.insert = insert;
    }

    public String getDelete() {
        return delete;
    }

    public void setDelete(String delete) {
        this.delete = delete;
    }

    public String getUpdate() {
        return update;
    }

    public void setUpdate(String update) {
        this.update = update;
    }
}
