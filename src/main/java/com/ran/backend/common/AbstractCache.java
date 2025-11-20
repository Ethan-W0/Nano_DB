package com.ran.backend.common;

import com.ran.common.Exceptions;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;

/**
 * AbstractCache实现一个引用计数策略的缓存
 */

public abstract class AbstractCache<T> {
    private HashMap<Long, T> cache;    //实际的缓存数据
    private HashMap<Long, Integer> references;    //引用计数
    private HashMap<Long, Boolean> getting;     //正在从数据源中获取

    private Lock lock;
    private int maxResource;        //缓存的最大缓存数量
    private int count = 0;         //已占用的缓存资源数（包括已加载和待加载的）


    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
    }

    /**
     * 当资源不在缓存时获取行为
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时，资源写回
     */
    protected abstract void releaseForCache(T obj);

    //从缓存中获取资源
    protected T get(long key) throws Exception {
        //无限循环，直到获取到资源为止
        while (true) {
            //获取锁
            lock.lock();
            //1.缓存中没有该数据，并且有其他线程正在查询
            if (getting.containsKey(key)) {
                //解除锁
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }
            //2.缓存中存在该该数据，直接返回资源，并且要增加引用计数
            if (cache.containsKey(key)) {
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }
            //3.如果资源不在缓存中，尝试获取资源。如果缓存已满，抛出异常
            if (maxResource > 0 && maxResource == count) {
                lock.unlock();
                throw Exceptions.CacheFullException;
            }
            //在缓存miss中，需要先count++占位，再加载进程
            count++;
            getting.put(key, true);
            lock.unlock();
            break;
        }
        //当资源不存在的时候获取数据源，直接调用getForCache方法
        T obj = null;
        try {
            obj = getForCache(key);
        } catch (Exception e) {
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }
        //将获取到的资源添加到缓存中，并设置引用计数为1
        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();

        return obj;
    }


    //释放一个缓存
    protected void realse(long key) {
        lock.lock();
        try{
            int ref = references.get(key)-1;
            if(ref == 0 ){
                //释放资源
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);         //从引用计数的映射中移除资源
                cache.remove(key);              //从缓存中释放资源
                count--;
            }else {
                references.put(key,ref);
            }
        }finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    protected void close(){
        lock.lock();
        try{
            //获取所有key的视图
            Set<Long> key = cache.keySet();
            for (Long k : key) {
                //释放资源
                T obj = cache.get(k);
                releaseForCache(obj);
                references.remove(k);
                cache.remove(k);
            }
        }finally {
            lock.unlock();
        }
    }

}










