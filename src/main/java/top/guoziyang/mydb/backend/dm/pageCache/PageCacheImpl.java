package top.guoziyang.mydb.backend.dm.pageCache;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.common.AbstractCache;
import top.guoziyang.mydb.backend.dm.page.Page;
import top.guoziyang.mydb.backend.dm.page.PageImpl;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.common.Error;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {
    
    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;

    private AtomicInteger pageNumbers;

    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        if(maxResource < MEM_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int)length / PAGE_SIZE);
    }

    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet();
        //新建page实例，参数为页码，数据，cache
        //因为新建所以cache为null
        Page pg = new PageImpl(pgno, initData, null);
        flush(pg);
        return pgno;
    }

    public Page getPage(int pgno) throws Exception {
        //调用泛型抽象cache的get
        return get((long)pgno);
    }

    /**
     * 根据pageNumber从数据库文件中读取页数据，并包裹成Page
     */
    //重写父类抽象类里面的从data get到cache
    @Override
    protected Page getForCache(long key) throws Exception {
        //以pageNo为key
        int pgno = (int)key;
        //偏移量
        long offset = PageCacheImpl.pageOffset(pgno);
        //预留一页大小
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            //位移，读取
            fc.position(offset);
            fc.read(buf);
        } catch(IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();
        //页面应有的属性：pageCache，用来release cache
        return new PageImpl(pgno, buf.array(), this);
    }

    //将脏页面保存
    @Override
    protected void releaseForCache(Page pg) {
        if(pg.isDirty()) {
            flush(pg);
            pg.setDirty(false);
        }
    }
    
    public void release(Page page) {
        //源于抽象cache的强行release
        //release的逻辑是传入pageNo对应的key
        release((long)page.getPageNumber());
    }

    public void flushPage(Page pg) {
        flush(pg);
    }

    private void flush(Page pg) {
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);

        fileLock.lock();
        try {
            //将当前data写入page
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }

    public void truncateByBgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);
        try {
            //文件截断到maxPgno为止（包含）
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        //改变记录的最大页数
        pageNumbers.set(maxPgno);
    }

    @Override
    public void close() {
        //删除cache内容
        super.close();
        try {
            //关掉file
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    private static long pageOffset(int pgno) {
        return (pgno-1) * PAGE_SIZE;
    }
    
}
