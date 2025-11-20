package com.ran.tm;

public interface TransactionManage {
    long begin();
    void abort(long xid);
    void commit(long xid);
    boolean isActive(long xid);
    boolean isCommitted(long xid);
    boolean isAborted(long xid);
    void close();

}
