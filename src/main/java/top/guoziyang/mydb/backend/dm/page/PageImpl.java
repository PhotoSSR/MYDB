package top.guoziyang.mydb.backend.dm.page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;

public class PageImpl implements Page {
    private int pageNumber;
    private byte[] data;
    private boolean dirty;
    private Lock lock;
    
    private PageCache pc;
//构造函数需要所在number，data，cache的引用
//且新建锁
//dirty默认false即可
    public PageImpl(int pageNumber, byte[] data, PageCache pc) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pc = pc;
        lock = new ReentrantLock();
    }
//实现interface
    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }
//释放cache即由cache对象release，参数是当前pageImp
    public void release() {
        //page的cache不一定相同
        //因此需要在page里面存cache的引用
        pc.release(this);
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public boolean isDirty() {
        return dirty;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public byte[] getData() {
        return data;
    }

}
