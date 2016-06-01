/**
 * Created by jeremyjiang on 2016/5/20.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.util;

import java.awt.image.BufferedImage;

import java.io.*;

import java.util.ArrayList;
import java.util.List;

import javax.imageio.ImageIO;

import cn.pku.net.db.storm.ndvr.entity.SIFTSigEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoSIFTSigEntity;
import com.google.gson.Gson;
import org.apache.log4j.Logger;

import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.entity.KeyFrameEntity;
import cn.pku.net.db.storm.ndvr.image.analyze.sift.SIFT;
import cn.pku.net.db.storm.ndvr.image.analyze.sift.render.RenderImage;
import cn.pku.net.db.storm.ndvr.image.analyze.sift.scale.KDFeaturePoint;

/**
 * Description:
 *
 * @author jeremyjiang
 * Created at 2016/5/20 9:52
 */
public class LocalSigGenerator {
    private static final Logger logger = Logger.getLogger(GlobalSigGenerator.class);

    public static List<List<KDFeaturePoint>> generate(List<KeyFrameEntity> keyframeList) {
        List<List<KDFeaturePoint>> keyPoints = new ArrayList<List<KDFeaturePoint>>();

        if (null == keyframeList || keyframeList.isEmpty()) {
            return keyPoints;
        }

        for (int i = 0; i < keyframeList.size(); i++) {
            KeyFrameEntity keyframeEnt2  = keyframeList.get(i);
            String         keyframeFile2 = Const.CC_WEB_VIDEO.KEYFRAME_PATH_PREFIX
                                           + Integer.parseInt(keyframeEnt2.getVideoId()) / 100 + "/"
                                           + keyframeEnt2.getKeyFrameName();

            try {
                BufferedImage img2 = ImageIO.read(new File(keyframeFile2));
                RenderImage   ri2  = new RenderImage(img2);
                SIFT          sift = new SIFT();

                sift.detectFeatures(ri2.toPixelFloatArray(null));

                List<KDFeaturePoint> al2 = sift.getGlobalKDFeaturePoints();

                keyPoints.add(al2);
            } catch (IOException e) {
                logger.error("IO error when read image: " + keyframeFile2, e);
            }
        }

        return keyPoints;
    }

    public static List<SIFTSigEntity> getFromFile(String videoId) {
        List<SIFTSigEntity> siftSigs = null;
        // 指定时长的视频的SIFT标签文件
        String videoSIFTFilePath = Const.CC_WEB_VIDEO.SIFT_SIGNATURE_PATH_PREFIX + Integer.parseInt(videoId) / 100
                + "/" + videoId + ".txt";
        File videoSIFTFile = new File(videoSIFTFilePath);
        if (!videoSIFTFile.exists()) {
            return siftSigs;
        }
        try {
            BufferedReader reader = new BufferedReader(new FileReader(videoSIFTFile));
            String line = reader.readLine();
            // 文件的一行代表一个compare视频
            if (null != line) {
                // compare视频的SIFT标签
                VideoSIFTSigEntity videoLocalSig = (new Gson()).fromJson(line, VideoSIFTSigEntity.class);
                // 如果比较视频的局部标签不存在,则继续处理下个视频
                if ((null == videoLocalSig) || videoLocalSig.getSignature().isEmpty()) {
                    return siftSigs;
                }
                // 视频各个帧图像的SIFT标签
                siftSigs = videoLocalSig.getSignature();
                reader.close();
            }
        } catch (FileNotFoundException e) {
            logger.error("file not found: " + videoSIFTFilePath, e);
        } catch (IOException e) {
            logger.error("io error when read file: " + videoSIFTFilePath, e);
        }
        return siftSigs;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
