/**
 * @Package cn.pku.net.db.storm.ndvr.util
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import cn.pku.net.db.storm.ndvr.common.Const;

/**
 * Description: Thread pool
 * @author jeremyjiang
 * Created at 2016/5/12 17:15
 */
public class MyThreadPool {
    private static Logger                logger   = Logger.getLogger(MyThreadPool.class);
    private static final MyThreadPool    INSTANCE = new MyThreadPool();
    private static final ExecutorService pool     = Executors.newFixedThreadPool(Const.THREAD_POOL_MAX_NUM);

    private MyThreadPool() {}

    /**
     * Gets instance.
     *
     * @return the instance
     */
    public static MyThreadPool getInstance() {
        return INSTANCE;
    }

    /**
     * Gets thread pool.
     *
     * @return the pool
     */
    public static ExecutorService getPool() {
        return pool;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
