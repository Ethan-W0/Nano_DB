package com.ran.backend.dm.pageCache;

import com.ran.backend.dm.page.Page;

import java.io.IOException;

public interface PageCache {
    public static final int PAGE_SIZE = 1 << 13;

    /**
     * 定义页面缓存接口，包括新建页面，获取，释放，关闭缓存、根据最大页号截断缓存、获取当前页面数量以及刷新页面等方法
     */
    int newPage(byte[] initData);
    Page getPage(int pgno) throws IOException;
    void close();
    void release(Page page);

    void truncateByBgno(int maxPgno);
    int getPageNumber();
    void flushPage(Page pg);




}
