package com.netease.kaola.distmerchant.common.util;

/**
 * @author kai.zhang
 * @since 2019/2/18
 */
public class PageParam {
    public static final int DEFAULT_PAGE_SIZE = 100;

    /**
     * 从0开始
     */
    private int pageStart = 0;
    private int pageSize;
    public PageParam() {

    }

    public PageParam(int pageStart, int pageSize) {
        this.pageStart = pageStart;
        this.pageSize = pageSize;
    }

    public int getPageStart() {
        return pageStart;
    }

    public void setPageStart(int pageStart) {
        this.pageStart = pageStart;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getLimitStart() {
        return (pageStart * pageSize);
    }
}
