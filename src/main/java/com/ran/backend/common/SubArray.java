package com.ran.backend.common;

/**
 * 共享数组
 */
public class SubArray {
    private byte[] raw;
    private int start;
    private int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
