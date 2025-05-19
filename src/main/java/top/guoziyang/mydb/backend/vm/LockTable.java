package top.guoziyang.mydb.backend.vm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.common.Error;

/**
 * 维护了一个依赖等待图，以进行死锁检测
 */
public class LockTable {
    
    private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表
    private Map<Long, Long> u2x;        // UID被某个XID持有
    private Map<Long, List<Long>> wait; // 正在等待UID的XID列表
    private Map<Long, Condition> waitCondition;   // 正在等待资源的XID的锁
    private Map<Long, Long> waitU;      // XID正在等待的UID
    private Lock lock;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitCondition = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    // 不需要等待则返回null，否则返回锁对象
    // 会造成死锁则抛出异常
    public Condition add(long xid, long uid) throws Exception {
        lock.lock();
        try {
            //已经得到锁，回null
            if(isInList(x2u, xid, uid)) {
                return null;
            }
            //uid空闲，双表登记，返回null
            if(!u2x.containsKey(uid)) {
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                return null;
            }
            //记录xid等待uid
            waitU.put(xid, uid);
            //记录uid被xid等待
            putIntoList(wait, uid, xid);
            //检测死锁，拒绝请求
            if(hasDeadLock()) {
                //要求xid释放所有资源
                waitU.remove(xid);
                removeFromList(wait, uid, xid);
                throw Error.DeadlockException;
            }
            //没有死锁，生成等待锁
            //这个等锁说明资源锁在lt中，lt分配时放开锁其他线程才能继续
            //返回锁
            Lock clock = new ReentrantLock();
            Condition condition = clock.newCondition();
            waitCondition.put(xid, condition);
            return condition;

        } finally {
            lock.unlock();
        }
    }
    
    //释放所有锁
    public void remove(long xid) {
        lock.lock();
        try {
            List<Long> l = x2u.get(xid);
            if(l != null) {
                while(l.size() > 0) {
                    //释放的uid再利用
                    Long uid = l.remove(0);
                    selectNewXID(uid);
                }
            }
            //删除xid的所有等待请求和已有资源
            waitU.remove(xid);
            x2u.remove(xid);
            waitCondition.remove(xid);

        } finally {
            lock.unlock();
        }
    }

    // 从等待队列中选择一个xid来占用uid
    private void selectNewXID(long uid) {
        u2x.remove(uid);
        //取出需要当前uid的所有xid
        List<Long> l = wait.get(uid);
        if(l == null) return;
        assert l.size() > 0;

        while(l.size() > 0) {
            long xid = l.remove(0);
            //公平锁，取第一个
            if(!waitCondition.containsKey(xid)) {
                continue;
            } else {
                //分配，记录在u-x
                //取消waitCondition，waitu
                //condition唤醒解锁
                u2x.put(uid, xid);
                Condition condition = waitCondition.remove(xid);
                waitU.remove(xid);
                condition.signalAll();
                break;
            }
        }
        //发生于唯一的等待者已经被刷掉，故删除
        if(l.size() == 0) wait.remove(uid);
    }

    private Map<Long, Integer> xidStamp;
    private int stamp;

    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        for(long xid : x2u.keySet()) {
            Integer s = xidStamp.get(xid);
            if(s != null && s > 0) {
                continue;
            }
            stamp ++;
            if(dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);
        if(stp != null && stp == stamp) {
            return true;
        }
        if(stp != null && stp < stamp) {
            return false;
        }
        xidStamp.put(xid, stamp);

        Long uid = waitU.get(xid);
        if(uid == null) return false;
        Long x = u2x.get(uid);
        assert x != null;
        return dfs(x);
    }

    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                i.remove();
                break;
            }
        }
        if(l.size() == 0) {
            listMap.remove(uid0);
        }
    }

    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if(!listMap.containsKey(uid0)) {
            listMap.put(uid0, new ArrayList<>());
        }
        listMap.get(uid0).add(0, uid1);
    }

    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return false;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                return true;
            }
        }
        return false;
    }

}
