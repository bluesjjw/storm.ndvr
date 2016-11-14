/**
 * @Package cn.pku.net.db.storm.ndvr.bolt
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */
package cn.pku.net.db.storm.ndvr.general;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.dao.HSVSignatureDao;
import cn.pku.net.db.storm.ndvr.dao.KeyFrameDao;
import cn.pku.net.db.storm.ndvr.dao.VideoInfoDao;
import cn.pku.net.db.storm.ndvr.entity.HSVSigEntity;
import cn.pku.net.db.storm.ndvr.entity.KeyFrameEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoHSVSigEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoInfoEntity;
import com.google.gson.Gson;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Description: General bolt, generate global visual signature
 *
 * @author jeremyjiang
 * Created at 2016/5/12 20:55
 */
public class GlobalSigBolt extends BaseBasicBolt {

    private static final Logger logger           = Logger.getLogger(GlobalSigBolt.class);
    private static Map<Integer, Set<String>> cachedVideoIdByDuration = new ConcurrentHashMap<Integer, Set<String>>();    // 缓存视频数据,key为duration,value为视频元数据
    private static Map<String, List<KeyFrameEntity>> cachedKeyFrame = new ConcurrentHashMap<String, List<KeyFrameEntity>>();   // 缓存的视频关键帧
    private static Map<String, HSVSigEntity> cachedHSVSignature = new ConcurrentHashMap<String, HSVSigEntity>();    // 缓存视频的HSV全局标签,key为视频id,value为视频HSV全局标签

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
                VideoHSVSigEntity videoHsvSig = (new HSVSignatureDao()).getVideoHSVSigById(videoInfoEnt.getVideoId());
                // 如果数据库中没有该视频的HSV标签,则继续下一个视频
                if (null == videoHsvSig) {
                    continue;
                }
                this.cachedHSVSignature.put(videoHsvSig.getVideoId(), videoHsvSig.getSig());    // 缓存时长为0的视频的HSV全局标签
                List<KeyFrameEntity> keyframeList = (new KeyFrameDao()).getKeyFrameByVideoId(videoInfoEnt.getVideoId());
                if (null == keyframeList || keyframeList.isEmpty()) {
                    continue;
                }
                this.cachedKeyFrame.put(videoInfoEnt.getVideoId(), keyframeList);
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
    public void execute(Tuple input, BasicOutputCollector collector) {
        String taskId = input.getStringByField("taskId");
        //logger.info("Global feature, taskId: " + taskId);
        String taskType = input.getStringByField("taskType");
        int fieldGroupingId = input.getIntegerByField("fieldGroupingId");
        Map<String, String> ctrlMsg = (Map<String, String>) input.getValue(3); //控制信息
        long startTime = System.currentTimeMillis();

        //retrieval任务,一个query视频
        if (Const.STORM_CONFIG.RETRIEVAL_TASK_FLAG.equals(taskType)) {
            String videoInfoStr = ctrlMsg.get("queryVideo");
            VideoInfoEntity videoInfoEnt = (new Gson()).fromJson(videoInfoStr, VideoInfoEntity.class);

            // 从缓存获取关键帧
            List<KeyFrameEntity> keyframeList = getKeyFrameFromCache(videoInfoEnt.getVideoId());

            //如果该视频没有对应的关键帧信息,则将全局标签设为null并输出
            if (null == keyframeList || keyframeList.isEmpty()) {
                //在控制信息中加入新字段
                ctrlMsg.put("globalSignature", null);
                //移除不必要的key
                if (Const.SSM_CONFIG.IS_REDUCTIION) {
                    ctrlMsg = StreamSharedMessage.discardInvalidKey("GlobalSigBolt", ctrlMsg);
                }
                //以duration做fieldGrouping,一个bolt负责的时长范围为Const.STORM_CONFIG.BOLT_DURATION_WINDOW
                collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
                return;
            }
            Collections.sort(keyframeList, new KeyFrameEntity());
            String keyframeListStr = (new Gson()).toJson(keyframeList);
            ctrlMsg.put("keyframeList", keyframeListStr);

            // 从缓存获取全局标签
            HSVSigEntity hsvSignature = getHSVSigFromCache(videoInfoEnt.getVideoId(), keyframeList);

            VideoHSVSigEntity videoHsvSig = new VideoHSVSigEntity(videoInfoEnt.getVideoId(),
                hsvSignature);
            String newGsonStr = (new Gson()).toJson(videoHsvSig);
            //在控制信息中加入新字段
            ctrlMsg.put("globalSignature", newGsonStr);
            //移除不必要的key
            if (Const.SSM_CONFIG.IS_REDUCTIION) {
                ctrlMsg = StreamSharedMessage.discardInvalidKey("GlobalSigBolt", ctrlMsg);
            }
            // time cost in this bolt
            logger.info(String.format("GlobalSigBolt cost %d ms", (System.currentTimeMillis() - startTime)));
            //以duration做fieldGrouping, 一个bolt负责的时长范围为Const.STORM_CONFIG.BOLT_DURATION_WINDOW
            collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
        }
        //detection任务,两个query视频
        else if (Const.STORM_CONFIG.DETECTION_TASK_FLAG.equals(taskType)) {
            // 如果使用pre-filtering的策略，若文本特征不相似，则不比较全局特征
            if (Const.STORM_CONFIG.IS_FILTER_AND_REFINE && ctrlMsg.containsKey("textSimilarity")
                    && Float.parseFloat(ctrlMsg.get("textSimilarity")) < Const.STORM_CONFIG.TEXT_SIMILARITY_THRESHOLD) {
                // 在控制信息中加入视频的全局标签,设为null
                ctrlMsg.put("globalSignature1", null);
                ctrlMsg.put("globalSignature2", null);
                // 移除不必要的key
                if (Const.SSM_CONFIG.IS_REDUCTIION) {
                    ctrlMsg = StreamSharedMessage.discardInvalidKey("GlobalSigSimilarBolt", ctrlMsg);
                }
                // 控制fieldGrouping,一个bolt负责一个时间段的视频,detection任务其实不需要fieldGrouping
                collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
                return;
            }
            String videoInfoStr1 = ctrlMsg.get("queryVideo1");
            VideoInfoEntity videoInfoEnt1 = (new Gson()).fromJson(videoInfoStr1, VideoInfoEntity.class);
            List<KeyFrameEntity> keyframeList1 = getKeyFrameFromCache(videoInfoEnt1.getVideoId());

            String videoInfoStr2 = ctrlMsg.get("queryVideo2");
            VideoInfoEntity videoInfoEnt2 = (new Gson()).fromJson(videoInfoStr2, VideoInfoEntity.class);
            List<KeyFrameEntity> keyframeList2 = getKeyFrameFromCache(videoInfoEnt2.getVideoId());

            //如果视频时长相差很大，或者该视频没有对应的关键帧信息,则输出null
            if (Math.abs(videoInfoEnt1.getDuration() - videoInfoEnt2.getDuration()) > Const.STORM_CONFIG.VIDEO_DURATION_WINDOW
                    || null == keyframeList1 || keyframeList1.isEmpty() || null == keyframeList2 | keyframeList2.isEmpty()) {
                // 在控制信息中加入视频的全局标签,设为null
                ctrlMsg.put("globalSignature1", null);
                ctrlMsg.put("globalSignature2", null);
                // 移除不必要的key
                if (Const.SSM_CONFIG.IS_REDUCTIION) {
                    ctrlMsg = StreamSharedMessage.discardInvalidKey("GlobalSigSimilarBolt", ctrlMsg);
                }
                // 控制fieldGrouping,一个bolt负责一个时间段的视频,detection任务其实不需要fieldGrouping
                collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
                return;
            }

            Collections.sort(keyframeList1, new KeyFrameEntity());
            String keyframeListStr1 = (new Gson()).toJson(keyframeList1);
            ctrlMsg.put("keyframeList1", keyframeListStr1);
            HSVSigEntity hsvSignature1 = getHSVSigFromCache(videoInfoEnt1.getVideoId(), keyframeList1);

            Collections.sort(keyframeList2, new KeyFrameEntity());
            String keyframeListStr2 = (new Gson()).toJson(keyframeList2);
            ctrlMsg.put("keyframeList2", keyframeListStr2);
            HSVSigEntity hsvSignature2 = getHSVSigFromCache(videoInfoEnt2.getVideoId(), keyframeList2);

            //如果生成全局标签失败,则输出null
            if (null == hsvSignature1 || null == hsvSignature2) {
                //在控制信息中加入视频的全局标签,设为null
                ctrlMsg.put("globalSignature1", null);
                ctrlMsg.put("globalSignature2", null);
                // 移除不必要的key
                if (Const.SSM_CONFIG.IS_REDUCTIION) {
                    ctrlMsg = StreamSharedMessage.discardInvalidKey("GlobalSigSimilarBolt", ctrlMsg);
                }
                //控制fieldGrouping,一个bolt负责一个时间段的视频,detection任务其实不需要fieldGrouping
                collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
                return;
            }
            VideoHSVSigEntity videoHsvSig1 = new VideoHSVSigEntity(videoInfoEnt1.getVideoId(), hsvSignature1);
            VideoHSVSigEntity videoHsvSig2 = new VideoHSVSigEntity(videoInfoEnt2.getVideoId(), hsvSignature2);
            //在控制信息中加入视频的全局标签
            ctrlMsg.put("globalSignature1", (new Gson()).toJson(videoHsvSig1));
            ctrlMsg.put("globalSignature2", (new Gson()).toJson(videoHsvSig2));
            //移除不必要的key
            if (Const.SSM_CONFIG.IS_REDUCTIION) {
                ctrlMsg = StreamSharedMessage.discardInvalidKey("GlobalSigBolt", ctrlMsg);
            }
            //控制fieldGrouping,一个bolt负责一个时间段的视频,detection任务其实不需要fieldGrouping
            collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
            //logger.info(String.format("Control message size in GlobalSigBolt: %d, taskId: %s",
            //        calMsgLength(ctrlMsg), taskId));
        }
    }

    private List<KeyFrameEntity> getKeyFrameFromCache (String videoId) {
        List<KeyFrameEntity> keyframeList = null;
        if (!cachedKeyFrame.containsKey(videoId)){
            keyframeList = (new KeyFrameDao()).getKeyFrameByVideoId(videoId);
            if (null != keyframeList && !keyframeList.isEmpty()) {
                cachedKeyFrame.put(videoId, keyframeList);
            }
        } else {
            keyframeList = cachedKeyFrame.get(videoId);
        }
        return keyframeList;
    }

    private HSVSigEntity getHSVSigFromCache (String videoId, List<KeyFrameEntity> keyframeList) {
        HSVSigEntity hsvSignature = null;
        if (!cachedHSVSignature.containsKey(videoId)){
            //hsvSignature = GlobalSigGenerator.generate(keyframeList);
            hsvSignature = (new HSVSignatureDao()).getVideoHSVSigById(videoId).getSig();
            if (null != hsvSignature) {
                cachedHSVSignature.put(videoId, hsvSignature);
            }
        } else {
            hsvSignature = cachedHSVSignature.get(videoId);
        }
        return hsvSignature;
    }

    /**
     * Cal SSM's length in bytes
     *
     * @param ctrlMsg the SSM ctrl message
     * @return the SSM's length
     */
    private int calMsgLength(Map<String, String> ctrlMsg) {
        int msgLength = 0;
        for (Map.Entry<String, String> entry : ctrlMsg.entrySet()) {
            msgLength += entry.getKey().length() + entry.getValue().length();
        }
        return msgLength;
    }
}
