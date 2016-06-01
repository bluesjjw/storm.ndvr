/**
 * @Package cn.pku.net.db.storm.ndvr.bolt
 * Created by jeremyjiang on 2016/5/13.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.general;

import java.awt.image.BufferedImage;

import java.io.File;
import java.io.IOException;

import java.lang.reflect.Type;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import javax.imageio.ImageIO;

import backtype.storm.task.TopologyContext;
import cn.pku.net.db.storm.ndvr.dao.HSVSignatureDao;
import cn.pku.net.db.storm.ndvr.dao.VideoInfoDao;
import cn.pku.net.db.storm.ndvr.entity.VideoHSVSigEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoInfoEntity;
import cn.pku.net.db.storm.ndvr.util.LocalSigGenerator;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

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

/**
 * Description: General bolt, generate local visual signature
 *
 * @author jeremyjiang
 * Created at 2016/5/13 9:41
 */
public class LocalSigBolt extends BaseBasicBolt {

    private static final Logger logger = Logger.getLogger(LocalSigBolt.class);
    private static Map<Integer, Set<String>> cachedVideoIdByDuration = new ConcurrentHashMap<Integer, Set<String>>();    // 缓存视频数据,key为duration,value为视频元数据
    private static Map<String, List<KeyFrameEntity>> cachedKeyFrame = new ConcurrentHashMap<String, List<KeyFrameEntity>>();   // 缓存的视频关键帧
    private static Map<String, List<List<KDFeaturePoint>>> cachedKeyPoints = new ConcurrentHashMap<String, List<List<KDFeaturePoint>>>();   // 缓存的SIFT标签

    /**
     * Prepare.
     *
     * @param stormConf the storm conf
     * @param context   the context
     * @see backtype.storm.topology.base.BaseBasicBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext) backtype.storm.topology.base.BaseBasicBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext)
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        VideoInfoDao videoInfoDao  = new VideoInfoDao();
        List<VideoInfoEntity> videoInfoList = videoInfoDao.getVideoInfoByDuration(0);    // 取出时长为0的视频(数据集中有些视频没有duration数据,我们设为0)
        if ((null != videoInfoList) && !videoInfoList.isEmpty()) {
            Set<String> videoIdSet = new HashSet<String>();
            for (VideoInfoEntity videoInfoEnt : videoInfoList) {
                videoIdSet.add(videoInfoEnt.getVideoId());
                List<KeyFrameEntity> keyframeList = (new KeyFrameDao()).getKeyFrameByVideoId(videoInfoEnt.getVideoId());
                if (null == keyframeList || keyframeList.isEmpty()) {
                    continue;
                }
                this.cachedKeyFrame.put(videoInfoEnt.getVideoId(), keyframeList);
                List<List<KDFeaturePoint>> keyPoints = LocalSigGenerator.generate(keyframeList);
                if (null != keyPoints && !keyPoints.isEmpty()){
                    cachedKeyPoints.put(videoInfoEnt.getVideoId(), keyPoints);
                }
            }
            this.cachedVideoIdByDuration.put(0, videoIdSet);                                    // 将时长为0的视频缓存
        }
    }

    /**
     * Declare output fields.
     *
     * @param declarer the declarer
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer) backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("taskId", "taskType", "fieldGroupingId", "ctrlMsg"));
    }

    /**
     * Execute.
     *
     * @param input     the input
     * @param collector the collector
     * @see backtype.storm.topology.IBasicBolt#execute(backtype.storm.tuple.Tuple, backtype.storm.topology.BasicOutputCollector) backtype.storm.topology.IBasicBolt#execute(backtype.storm.tuple.Tuple, backtype.storm.topology.BasicOutputCollector)
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String taskId = input.getStringByField("taskId");
        logger.info("Local feature, taskId: " + taskId);
        String              taskType        = input.getStringByField("taskType");
        int                 fieldGroupingId = input.getIntegerByField("fieldGroupingId");
        Map<String, String> ctrlMsg         = (Map<String, String>) input.getValue(3);    // 控制信息

        // retrieval任务,一个query视频
        if (Const.STORM_CONFIG.RETRIEVAL_TASK_FLAG.equals(taskType)) {
            String videoInfoStr = ctrlMsg.get("queryVideo");
            VideoInfoEntity videoInfoEnt = (new Gson()).fromJson(videoInfoStr, VideoInfoEntity.class);

            String               keyframeListStr  = ctrlMsg.get("keyframeList");
            Type                 keyframeListType = new TypeToken<ArrayList<KeyFrameEntity>>() { }.getType();
            List<KeyFrameEntity> keyframeList     = (new Gson()).fromJson(keyframeListStr, keyframeListType);

            // 保存视频的SIFT标签,list每个元素为一个帧的SIFT关键点
            List<List<KDFeaturePoint>> keyPoints = new ArrayList<List<KDFeaturePoint>>();

            if (null == keyframeList || keyframeList.isEmpty()) {
                // 在控制信息中加入局部关键点,可能为空!
                ctrlMsg.put("localSignature", (new Gson()).toJson(keyPoints));
                // 移除不必要的key
                ctrlMsg = StreamSharedMessage.discardInvalidKey("LocalSigBolt", ctrlMsg);
                collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
                return;
            }

            // 保存视频的SIFT标签,list每个元素为一个帧的SIFT关键点
            //keyPoints = LocalSigGenerator.generate(keyframeList);
            keyPoints = getKeyPointsFromCache(videoInfoEnt.getVideoId(), keyframeList);

            // 在控制信息中加入局部关键点,可能为空!
            ctrlMsg.put("localSignature", (new Gson()).toJson(keyPoints));

            // 移除不必要的key
            ctrlMsg = StreamSharedMessage.discardInvalidKey("LocalSigBolt", ctrlMsg);
            collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
            logger.info(String.format("Control message size in local feature distance: %d, taskId: %s",
                    StreamSharedMessage.calMsgLength(ctrlMsg), taskId));
        } else if (Const.STORM_CONFIG.DETECTION_TASK_FLAG.equals(taskType)) {
            Type                       keyframeListType = new TypeToken<ArrayList<KeyFrameEntity>>() { }.getType();
            // 在控制信息中加入第一个视频的局部关键点,可能为空!
            String keyframeListStr1 = ctrlMsg.get("keyframeList1");
            List<KeyFrameEntity>       keyframeList1     = (new Gson()).fromJson(keyframeListStr1, keyframeListType);
            List<List<KDFeaturePoint>> keyPoints1        = LocalSigGenerator.generate(keyframeList1);
            ctrlMsg.put("localSignature1", (new Gson()).toJson(keyPoints1));

            // 在控制信息中加入第二个视频的局部关键点,可能为空!
            String keyframeListStr2 = ctrlMsg.get("keyframeList2");
            List<KeyFrameEntity>       keyframeList2 = (new Gson()).fromJson(keyframeListStr2, keyframeListType);
            List<List<KDFeaturePoint>> keyPoints2    = LocalSigGenerator.generate(keyframeList2);
            ctrlMsg.put("localSignature2", (new Gson()).toJson(keyPoints2));

            // 移除不必要的key
            ctrlMsg = StreamSharedMessage.discardInvalidKey("LocalSigBolt", ctrlMsg);
            collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
            logger.info(String.format("Control message size in local feature distance: %d, taskId: %s",
                    StreamSharedMessage.calMsgLength(ctrlMsg), taskId));
        }
    }

    private List<List<KDFeaturePoint>> getKeyPointsFromCache (String videoId, List<KeyFrameEntity> keyframeList) {
        List<List<KDFeaturePoint>> keyPoints = null;
        if (!cachedKeyPoints.containsKey(videoId)){
            keyPoints = LocalSigGenerator.generate(keyframeList);
            if (null != keyPoints && !keyPoints.isEmpty()) {
                cachedKeyPoints.put(videoId, keyPoints);
            }
        } else {
            keyPoints = cachedKeyPoints.get(videoId);
        }
        return keyPoints;
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
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
                BufferedImage img  = ImageIO.read(new File(keyframeFile));
                RenderImage   ri   = new RenderImage(img);
                SIFT          sift = new SIFT();

                sift.detectFeatures(ri.toPixelFloatArray(null));

                List<KDFeaturePoint> al = sift.getGlobalKDFeaturePoints();

                keyPoints1.add(al);
            }

            for (KeyFrameEntity keyframeEnt : keyframeList2) {
                String keyframeFile = Const.CC_WEB_VIDEO.KEYFRAME_PATH_PREFIX
                                      + Integer.parseInt(keyframeEnt.getVideoId()) / 100 + "\\"
                                      + keyframeEnt.getKeyFrameName();
                File          file = new File(keyframeFile);
                BufferedImage img  = ImageIO.read(new File(keyframeFile));
                RenderImage   ri   = new RenderImage(img);
                SIFT          sift = new SIFT();

                sift.detectFeatures(ri.toPixelFloatArray(null));

                List<KDFeaturePoint> al = sift.getGlobalKDFeaturePoints();

                keyPoints2.add(al);
            }

            int window = 3;

            for (int i = 0; i < keyPoints1.size(); i++) {
                if (null == keyPoints1.get(i)) {
                    continue;
                }

                // for (int j = i - window; j < i + window; j++) {

                if (i <= keyPoints2.size()) {
                    List<KDFeaturePoint> a1 = keyPoints1.get(i);
                    List<KDFeaturePoint> a2 = keyPoints2.get(i);
                    List<Match>          ms = MatchKeys.findMatchesBBF(a1, a2);

                    ms = MatchKeys.filterMore(ms);
                }

                // }
            }
        } catch (IOException e) {
            logger.error("", e);
        }
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
