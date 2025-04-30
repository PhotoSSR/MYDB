package top.guoziyang.mydb.backend.dm.page;

import java.util.Arrays;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.utils.RandomUtil;

/**
 * 特殊管理第一页
 * ValidCheck
 * db启动时给100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 * 用于判断上一次数据库是否正常关闭
 */
public class PageOne {
    //偏移量
    private static final int OF_VC = 100;
    //验证码长度
    private static final int LEN_VC = 8;

    public static byte[] InitRaw() {
        //获取初始raw
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    //传入page
    //一般是第一页
    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        //getData返回的是所有数据的一个byte【】
        //长度pagesize
        setVcOpen(pg.getData());
    }

    private static void setVcOpen(byte[] raw) {
        //被copy数组，被copy起始位置，copy到数组，copy到起始位置，copy长度
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC+LEN_VC, LEN_VC);
    }

    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    private static boolean checkVc(byte[] raw) {
        //判断ofvc到ofvc+len即开局验证码
        //以及ofvc+len到ofvc+2len即结束验证码
        //是否相等
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC+LEN_VC), Arrays.copyOfRange(raw, OF_VC+LEN_VC, OF_VC+2*LEN_VC));
    }
}
