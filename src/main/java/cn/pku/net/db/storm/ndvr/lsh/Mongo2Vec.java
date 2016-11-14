/**
 * Created by jeremyjiang on 2016/6/17.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */
package cn.pku.net.db.storm.ndvr.lsh;

import cn.pku.net.db.storm.ndvr.dao.HSVSignatureDao;
import cn.pku.net.db.storm.ndvr.dao.VideoInfoDao;
import cn.pku.net.db.storm.ndvr.entity.VideoHSVSigEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoInfoEntity;

import java.util.ArrayList;
import java.util.List;

/**
 * Description:
 *
 * @author jeremyjiang
 * Created at 2016/6/17 12:59
 */

public class Mongo2Vec {

    public static List<Vector> getAllGlobalSigAsVec() {
        List<Vector> vectors = new ArrayList<Vector>();

        VideoInfoDao videoInfoDao = new VideoInfoDao();
        HSVSignatureDao hsvSigDao = new HSVSignatureDao();
        List<VideoInfoEntity> videoList = videoInfoDao.getAllVideoInfo();
        for (VideoInfoEntity video : videoList) {
            VideoHSVSigEntity hsvSig = hsvSigDao.getVideoHSVSigById(video.getVideoId());
            if (null == hsvSig) {
                System.out.println("No HSV signature found of video: " + video.getVideoId());
                continue;
            }
            int dimension = 24;
            Vector sigVec = new Vector(dimension);
            sigVec.setKey(hsvSig.getVideoId());
            for (int i = 0; i < 24; i++) {
                sigVec.set(i, hsvSig.getSig().getBins()[i]);
            }
            vectors.add(sigVec);
        }
        return vectors;
    }

    public static Vector convert(VideoHSVSigEntity hsvSig) {
        int dimension = 24;
        Vector sigVec = new Vector(dimension);
        sigVec.setKey(hsvSig.getVideoId());
        for (int i = 0; i < 24; i++) {
            sigVec.set(i, hsvSig.getSig().getBins()[i]);
        }
        return sigVec;
    }
}