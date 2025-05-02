package top.guoziyang.mydb.backend.vm;

import top.guoziyang.mydb.backend.tm.TransactionManager;
//检查可见性
public class Visibility {
    //是否发生版本跳跃
    //最新版已经出现，且当前事务不可见
    //那么修改后的新新版就会跳过最新版
    //即为版本跳跃
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if(t.level == 0) {
            //事务本身无效
            return false;
        } else {
            //前一个改变已经提交
            //且xmax对xid不可见
            //说明存在一个未知版本
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        //level是隔离等级，0表示读已提交
        if(t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    //读已提交
    //解读注意点：返回的是当前e对当前tx是否可见
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        //e是当前tx提交的最新版，当然可见
        if(xmin == xid && xmax == 0) return true;

        //e是其他tx且已经提交
        if(tm.isCommitted(xmin)) {
            //还没被修改的最新版，可见
            if(xmax == 0) return true;
            //已经修改
            if(xmax != xid) {
                //如果新修改已经commit，则该e过期，不可见
                //所以判断！committed
                if(!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

    //可重复读
    //需要维护一个并行事务表
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        //当前所属可见
        if(xmin == xid && xmax == 0) return true;

        //e来自其他事务且在tx之前就已经完成
        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            //且未被修改可见
            if(xmax == 0) return true;
            //或且修改者没提交说明修改还无效，所以当前e可见
            //上条覆盖读已提交

            //或且修改者在当前id之后，所以修改无效，e可见
            //或且修改者在快照列表内
            //虽然修改已提交，但是被id名单隔离

            //故当前e仍然可见
            if(xmax != xid) {
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

}
