/**
 * @Title: SIFTSignatureDao.java
 * @Package cn.pku.net.db.storm.ndvr.dao
 * @Description: TODO
 * @author Jiawei Jiang
 * @date 2015年1月5日 上午10:18:33
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved.
 */



package cn.pku.net.db.storm.ndvr.dao;

import java.net.UnknownHostException;

import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import com.mongodb.MongoClient;

import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.entity.KeyFrameEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoInfoEntity;
import cn.pku.net.db.storm.ndvr.util.MyThreadPool;
import cn.pku.net.db.storm.ndvr.util.SIFTSigToFile;

/**
 * Description: Data access object for SIFT signature
 *
 * @author jeremyjiang
 * Created at 2016/5/12 19:46
 */
public class SIFTSignatureDao {
    private static final Logger logger      = Logger.getLogger(SIFTSignatureDao.class);
    private static MongoClient  mongoClient = null;

    /**
     * Instantiates
     */
    public SIFTSignatureDao() {
        if (null == mongoClient) {
            try {
                mongoClient = new MongoClient(Const.MONGO.MONGO_HOST, Const.MONGO.MONGO_PORT);
            } catch (UnknownHostException e) {
                logger.error("MongoDB UnknownHost", e);
            }
        }
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {
        SIFTSignatureDao dao = new SIFTSignatureDao();

        dao.saveToFile();

        // 11742/11745/11751
    }

    /**
     * Calculate the SIFT signatures of all the videos and save them to files
     */
    public void saveToFile() {
        List<VideoInfoEntity> videoList = (new VideoInfoDao()).getAllVideoInfo();

        for (VideoInfoEntity videoInfo : videoList) {

            // if (doneFileList.contains(videoInfo.getVideoId())) {
            ////                System.out.println("Video has been processed: " + videoInfo.getVideoId());
            // continue;
            // }
            List<KeyFrameEntity> keyframeList = (new KeyFrameDao()).getKeyFrameByVideoId(videoInfo.getVideoId());

            Collections.sort(keyframeList, new KeyFrameEntity());

            Runnable r = new SIFTSigToFile(videoInfo, keyframeList);

            MyThreadPool.getPool().submit(r);
        }
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
