package top.guoziyang.mydb.backend.dm.pageIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;

public class PageIndex {
    // 将一页划成40个区间
    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    private List<PageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO+1];
        for (int i = 0; i < INTERVALS_NO+1; i ++) {
            lists[i] = new ArrayList<>();
        }
    }

    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            //添加信息，看看对应页面应该放哪 
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            //需要多少区间
            int number = spaceSize / THRESHOLD;
            
            //因为索引是按照从1开始
            if(number < INTERVALS_NO) number ++;
            while(number <= INTERVALS_NO) {
                //没有能正好容纳的页，则再++，寻找更大页
                if(lists[number].size() == 0) {
                    number ++;
                    continue;
                }
                //取出第一个可用页面的信息你 
                return lists[number].remove(0);
            }
            
            return null;
        } finally {
            lock.unlock();
        }
    }

}
