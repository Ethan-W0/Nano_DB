package com.ran.backend.dm.pageCache;

import com.ran.backend.common.AbstractCache;
import com.ran.backend.dm.page.Page;
import com.ran.backend.dm.page.PageImpl;
import com.ran.backend.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {
    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;

    private AtomicInteger pageNumbers;


    @Override
    public int newPage(byte[] initData) {
        return 0;
    }

    @Override
    public Page getPage(int pgno) throws IOException {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void release(Page page) {

    }

    @Override
    public void truncateByBgno(int maxPgno) {

    }

    @Override
    public int getPageNumber() {
        return 0;
    }

    @Override
    public void flushPage(Page pg) {

    }

    /**
     * 从文件中读取，并且包裹成Page
     * @param key
     * @return
     * @throws Exception
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int)key;
        long offset = PageCacheImpl.pageOffset(pgno);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(buf);
        }catch (IOException e) {
            Panic.panic(e);
        }

        fileLock.unlock();
        return new PageImpl(pgno,buf.array(),this);
    }

    private static long pageOffset(int pgno) {
        return (pgno-1)*PAGE_SIZE;
    }


    /**
     * 先判断该页面是否为脏页面，然后来决定是否需要写回文件系统
     * @param pg
     */
    @Override
    protected void releaseForCache(Page pg) {
        if(pg.isDirty()){
            flush(pg);
            pg.setDirty(false);
        }
    }

    private void flush(Page pg){
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);

        fileLock.lock();
        try{
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        }catch (IOException e){
            Panic.panic(e);
        }
    }
}
