/**
 * @Title: LocalFeatureBolt.java 
 * @Package cn.pku.net.db.storm.ndvr.bolt 
 * @Description: TODO
 * @author Jiawei Jiang    
 * @date 2015年1月3日 上午9:24:52 
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved.
 */
package cn.pku.net.db.storm.ndvr.bolt;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import org.apache.log4j.Logger;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.dao.KeyFrameDao;
import cn.pku.net.db.storm.ndvr.entity.KeyFrameEntity;
import cn.pku.net.db.storm.ndvr.image.analyze.sift.SIFT;
import cn.pku.net.db.storm.ndvr.image.analyze.sift.match.Match;
import cn.pku.net.db.storm.ndvr.image.analyze.sift.match.MatchKeys;
import cn.pku.net.db.storm.ndvr.image.analyze.sift.render.RenderImage;
import cn.pku.net.db.storm.ndvr.image.analyze.sift.scale.KDFeaturePoint;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * @ClassName: LocalFeatureBolt
 * @Description: TODO
 * @author Jiawei Jiang
 * @date 2015年1月3日 上午9:24:52
 */
public class LocalFeatureRetrievalBolt extends BaseBasicBolt {

    private static final Logger logger = Logger.getLogger(LocalFeatureRetrievalBolt.class);

    /** 
     * @see backtype.storm.topology.IBasicBolt#execute(backtype.storm.tuple.Tuple, backtype.storm.topology.BasicOutputCollector)
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String taskId = input.getStringByField("taskId");
        logger.info("Local feature, taskId: " + taskId);
        String taskType = input.getStringByField("taskType");
        int fieldGroupingId = input.getIntegerByField("fieldGroupingId");
    Map<String, String> ctrlMsg = (Map<String, String>) input.getValue(3); // 控制信息
    // retrieval任务,一个query视频
        if (Const.STORM_CONFIG.RETRIEVAL_TASK_FLAG.equals(taskType)) {
            String keyframeListStr = ctrlMsg.get("keyframeList");
            Type keyframeListType = new TypeToken<ArrayList<KeyFrameEntity>>() {
            }.getType();
            List<KeyFrameEntity> keyframeList = (new Gson()).fromJson(keyframeListStr,
                keyframeListType);
      // 保存视频的SIFT标签,list每个元素为一个帧的SIFT关键点
            List<List<KDFeaturePoint>> keyPoints = new ArrayList<List<KDFeaturePoint>>();
            for (int i = 0; i < keyframeList.size(); i++) {
                KeyFrameEntity keyframeEnt = keyframeList.get(i);
                String keyframeFile = Const.CC_WEB_VIDEO.KEYFRAME_PATH_PREFIX
                                      + Integer.parseInt(keyframeEnt.getVideoId()) / 100 + "/"
                                      + keyframeEnt.getKeyFrameName();
                try {
                    BufferedImage img = ImageIO.read(new File(keyframeFile));
                    RenderImage ri = new RenderImage(img);
                    SIFT sift = new SIFT();
                    sift.detectFeatures(ri.toPixelFloatArray(null));
                    List<KDFeaturePoint> al = sift.getGlobalKDFeaturePoints();
                    keyPoints.add(al);
                } catch (IOException e1) {
                    logger.error("IO error when read image: " + keyframeFile, e1);
                } catch (java.lang.ArrayIndexOutOfBoundsException e2) {
                    logger.error("Array index out of bounds: " + keyframeFile, e2);
                }
            }
      // 在控制信息中加入局部关键点,可能为空!
            String keyPointsStr = (new Gson()).toJson(keyPoints);
            ctrlMsg.put("localSignature", keyPointsStr);
            //            logger.info("put localSignature: " + keyPointsStr);
      // 移除不必要的key
            if (Const.SSM_CONFIG.IS_REDUCTIION) {
                ctrlMsg = Const.SSM_CONFIG.discardInvalidKey("LocalFeatureRetrievalBolt",
                    ctrlMsg);
            }
            collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
            int msgLength = 0;
            for (Map.Entry<String, String> entry : ctrlMsg.entrySet()) {
                msgLength += entry.getKey().length() + entry.getValue().length();
            }
            logger.info("Control message size in local feature: " + msgLength + ", taskId: "
                        + taskId);
        } else if (Const.STORM_CONFIG.DETECTION_TASK_FLAG.equals(taskType)) {
            String keyframeListStr = ctrlMsg.get("keyframeList");
            //            logger.info("keyframeList" + keyframeListStr);
            Type keyframeListType = new TypeToken<ArrayList<KeyFrameEntity>>() {
            }.getType();
            List<KeyFrameEntity> keyframeList = (new Gson()).fromJson(keyframeListStr,
                keyframeListType);
            List<List<KDFeaturePoint>> keyPoints = new ArrayList<List<KDFeaturePoint>>();
            for (int i = 0; i < keyframeList.size(); i++) {
                KeyFrameEntity keyframeEnt = keyframeList.get(i);
                String keyframeFile = Const.CC_WEB_VIDEO.KEYFRAME_PATH_PREFIX
                                      + Integer.parseInt(keyframeEnt.getVideoId()) / 100 + "/"
                                      + keyframeEnt.getKeyFrameName();
                try {
                    BufferedImage img = ImageIO.read(new File(keyframeFile));
                    RenderImage ri = new RenderImage(img);
                    SIFT sift = new SIFT();
                    sift.detectFeatures(ri.toPixelFloatArray(null));
                    List<KDFeaturePoint> al = sift.getGlobalKDFeaturePoints();
                    keyPoints.add(al);
                } catch (IOException e) {
                    logger.error("IO error when read image: " + keyframeFile, e);
                }
            }
      // 在控制信息中加入第一个视频的局部关键点,可能为空!
            String keyPointsStr = (new Gson()).toJson(keyPoints);
            ctrlMsg.put("localSignature", keyPointsStr);
            //            logger.info("put localSignature: " + keyPointsStr);

            String keyframeListStr2 = ctrlMsg.get("keyframeList2");
            //            logger.info("keyframeList2: " + keyframeListStr2);
            List<KeyFrameEntity> keyframeList2 = (new Gson()).fromJson(keyframeListStr2,
                keyframeListType);
            List<List<KDFeaturePoint>> keyPoints2 = new ArrayList<List<KDFeaturePoint>>();
            for (int i = 0; i < keyframeList2.size(); i++) {
                KeyFrameEntity keyframeEnt2 = keyframeList2.get(i);
                String keyframeFile2 = Const.CC_WEB_VIDEO.KEYFRAME_PATH_PREFIX
                                       + Integer.parseInt(keyframeEnt2.getVideoId()) / 100 + "\\"
                                       + keyframeEnt2.getKeyFrameName();
                try {
                    BufferedImage img2 = ImageIO.read(new File(keyframeFile2));
                    RenderImage ri2 = new RenderImage(img2);
                    SIFT sift = new SIFT();
                    sift.detectFeatures(ri2.toPixelFloatArray(null));
                    List<KDFeaturePoint> al2 = sift.getGlobalKDFeaturePoints();
                    keyPoints2.add(al2);
                } catch (IOException e) {
                    logger.error("IO error when read image: " + keyframeFile2, e);
                }
            }
      // 在控制信息中加入第二个视频的局部关键点,可能为空!
            String keyPointsStr2 = (new Gson()).toJson(keyPoints2);
            ctrlMsg.put("localSignature2", keyPointsStr2);
            //            logger.info("put localSignature2: " + keyPointsStr2);

            collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
        }
    }

    /** 
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("taskId", "taskType", "fieldGroupingId", "ctrlMsg"));
    }

    /**
     * @Title: main 
     * @Description: TODO
     * @param @param args     
     * @return void   
     * @throws 
     * @param args
     */
    public static void main(String[] args) {
        try {
            List<KeyFrameEntity> keyframeList1 = (new KeyFrameDao()).getKeyFrameByVideoId("1");
            List<KeyFrameEntity> keyframeList2 = (new KeyFrameDao()).getKeyFrameByVideoId("7");
            Collections.sort(keyframeList1, new KeyFrameEntity());
            Collections.sort(keyframeList2, new KeyFrameEntity());
            List<List<KDFeaturePoint>> keyPoints1 = new ArrayList<List<KDFeaturePoint>>();
            List<List<KDFeaturePoint>> keyPoints2 = new ArrayList<List<KDFeaturePoint>>();
            for (KeyFrameEntity keyframeEnt : keyframeList1) {
                String keyframeFile = Const.CC_WEB_VIDEO.KEYFRAME_PATH_PREFIX
                                      + Integer.parseInt(keyframeEnt.getVideoId()) / 100 + "\\"
                                      + keyframeEnt.getKeyFrameName();
                BufferedImage img = ImageIO.read(new File(keyframeFile));
                RenderImage ri = new RenderImage(img);
                SIFT sift = new SIFT();
                sift.detectFeatures(ri.toPixelFloatArray(null));
                List<KDFeaturePoint> al = sift.getGlobalKDFeaturePoints();
                keyPoints1.add(al);
            }
            for (KeyFrameEntity keyframeEnt : keyframeList2) {
                String keyframeFile = Const.CC_WEB_VIDEO.KEYFRAME_PATH_PREFIX
                                      + Integer.parseInt(keyframeEnt.getVideoId()) / 100 + "\\"
                                      + keyframeEnt.getKeyFrameName();
                File file = new File(keyframeFile);

                BufferedImage img = ImageIO.read(new File(keyframeFile));
                RenderImage ri = new RenderImage(img);
                SIFT sift = new SIFT();
                sift.detectFeatures(ri.toPixelFloatArray(null));
                List<KDFeaturePoint> al = sift.getGlobalKDFeaturePoints();
                keyPoints2.add(al);
            }

            int window = 3;

            for (int i = 0; i < keyPoints1.size(); i++) {
                if (null == keyPoints1.get(i)) {
                    continue;
                }
                //                for (int j = i - window; j < i + window; j++) {

                if (i <= keyPoints2.size()) {
                    List<KDFeaturePoint> a1 = keyPoints1.get(i);
                    List<KDFeaturePoint> a2 = keyPoints2.get(i);
                    List<Match> ms = MatchKeys.findMatchesBBF(a1, a2);
                    ms = MatchKeys.filterMore(ms);
                }
                //                              }  
            }
        } catch (IOException e) {
            logger.error("", e);
        }
    }
}
