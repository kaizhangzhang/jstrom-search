package com.netease.kaola.distmerchant.common.util;

/**
 * @author kai.zhang
 * @since 2019/2/2
 */
public class LogInfoFormat {
    public static String formatSpoutLog(String spoutId) {
        return String.format("Thread: %s-%s, SpoutId: %s ", Thread.currentThread().getName(), Thread.currentThread().getId(), spoutId);
    }

    public static String formatBoltLog(String boltId) {
        return String.format("Thread: %s-%s BoltId: %s ", Thread.currentThread().getName(), Thread.currentThread().getId(), boltId);
    }
}
