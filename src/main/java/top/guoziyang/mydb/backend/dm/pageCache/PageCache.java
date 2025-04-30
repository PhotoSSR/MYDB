package top.guoziyang.mydb.backend.dm.pageCache;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import top.guoziyang.mydb.backend.dm.page.Page;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.common.Error;

public interface PageCache {
    //pagecache是什么
    //规定page的size
    public static final int PAGE_SIZE = 1 << 13;
    //要有new方法
    int newPage(byte[] initData);
    //要有get方法
    Page getPage(int pgno) throws Exception;
    //close和release方法
    void close();
    void release(Page page);

    void truncateByBgno(int maxPgno);
    int getPageNumber();
    void flushPage(Page pg);
    //两个静态方法分别是打开和新建cache
    //返回一个cache实例
    //create Imp
    public static PageCacheImpl create(String path, long memory) {
        //new一个后缀为".db"的文件
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        //先检验是否已存在
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        //检验权限
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        //用fc处理更方便
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            //由file赋值raf
            //raf赋值fc
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
           Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }

    public static PageCacheImpl open(String path, long memory) {
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        //不存在就报错
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
           Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }
}
