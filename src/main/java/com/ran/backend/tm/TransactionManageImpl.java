package com.ran.backend.tm;

import com.ran.common.Exceptions;
import com.ran.backend.utils.Panic;
import com.ran.backend.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;

public class TransactionManageImpl implements TransactionManage{
    //XID文件头长度
    static final int LEN_XID_HEADER_LENGTH = 8;
    //每个事务所占用的长度(状态)
    private static final int LEN_XID_BODY_LENGTH = 1;
    //事务的三个状态
    private static final byte XID_BODY_ACTIVE=0;
    private static final byte XID_BODY_COMMITTED=1;
    private static final byte XID_BODY_ABORTED=2;
    //超级事务.一直为已提交状态
    public static final int SUPER_XID=1;
    //XID文件后缀为.xid
    static final String XID_SUFFIX=".xid";
    //通过RandomAccessFile读取文件，以及FileChannel写入文件
    private RandomAccessFile file;
    private FileChannel fc;
    private long xidCounter;
    private Lock counterLock;


    //检查XID文件是否合法
    private void checkXidCounter(){
        long fileLen = 0 ;
        try{
            fileLen = file.length();
        }catch (IOException e1){
            Panic.panic(Exceptions.BadXidException);
        }
        if(fileLen != LEN_XID_HEADER_LENGTH){
            Panic.panic(Exceptions.BadXidException);
        }
        //接下来需要结合XidCounter反推文件总长度
        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try{
            //从文件的第一个字节开始读
            fc.position(0);
            //将读取到的内容写入到buf里
            fc.read(buf);
        }catch(IOException e){
            Panic.panic(e);
        }
        this.xidCounter = Parser.parseLong(buf.array());
        long end = getXidPosition(this.xidCounter+1);
        if(end != fileLen){
            Panic.panic(Exceptions.BadXidException);
        }

    }
    public long getXidPosition(long xid){
        return LEN_XID_HEADER_LENGTH+(xid-1)*LEN_XID_BODY_LENGTH;
    }

    //开始一个事务，并且返回XID
    public long begin() {
        //新建一个事务，要加锁
        counterLock.lock();
        //每开始一个新的事务，就将其+1
        try{
            long xid = xidCounter + 1;
            //更新事务状态
            updateXid(xid,XID_BODY_ACTIVE);
            //将事务计数器+1，并更新XID文件的头部信息
            incrXIDCounter();
            return xid;
        } finally {
            //释放锁
            counterLock.unlock();
        }
    }

    private void incrXIDCounter() {
        //事务数+1
        xidCounter++;
        //将xidCounter写入到xid文件前面
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try{
            fc.position(0);
            fc.write(buf);
        }catch (IOException e){
            Panic.panic(e);
        }
        try{
            fc.force(false);
        }catch (IOException e){
            Panic.panic(e);
        }
    }

    //注意：status应该用byte
    private void updateXid(long xid, byte status) {
        //先找到事务在文件中的位置
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[LEN_XID_BODY_LENGTH];
        tmp[0] = status;

        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try{
            fc.position(offset);
            fc.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }
        try{
            //强制将文件通道中所有未写入的数据写入到磁盘
            fc.force(false);
        }catch (IOException e){
            Panic.panic(e);
        }
    }

    private boolean checkXid(long xid , byte status){
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[LEN_XID_BODY_LENGTH];
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try{
            fc.position(offset);
            fc.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }
    @Override
    public void abort(long xid) {
        updateXid(xid,XID_BODY_ABORTED);
    }

    @Override
    public void commit(long xid) {
        updateXid(xid,XID_BODY_COMMITTED);
    }

    @Override
    public boolean isActive(long xid) {
        if(xid==SUPER_XID) return false;
        return checkXid(xid,XID_BODY_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if(xid == SUPER_XID) return true;
        return checkXid(xid, XID_BODY_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXid(xid, XID_BODY_ABORTED);
    }

    @Override
    public void close() {
        try{
            fc.close();
            file.close();
        }catch (IOException e){
            Panic.panic(e);
        }

    }
}
