package top.guoziyang.mydb.backend.dm.page;
//interface，只定义page的基本构造
//完成锁，释放，dirty，get
public interface Page {
    void lock();
    void unlock();
    void release();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    byte[] getData();
}
