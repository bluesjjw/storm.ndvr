/**
 * @Package cn.pku.net.db.storm.ndvr.bolt
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */
package cn.pku.net.db.storm.ndvr.general;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.dao.KeyFrameDao;
import cn.pku.net.db.storm.ndvr.entity.HSVSigEntity;
import cn.pku.net.db.storm.ndvr.entity.KeyFrameEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoHSVSigEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoInfoEntity;
import cn.pku.net.db.storm.ndvr.util.GlobalSigGenerator;

import com.google.gson.Gson;

/**
 * Description: General bolt, generate global visual signature
 *
 * @author jeremyjiang
 * Created at 2016/5/12 20:55
 */
public class GlobalFeatureBolt extends BaseBasicBolt {

    private static final Logger logger           = Logger.getLogger(GlobalFeatureBolt.class);

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
        logger.info("Global feature, taskId: " + taskId);
        String taskType = input.getStringByField("taskType");
        int fieldGroupingId = input.getIntegerByField("fieldGroupingId");
        Map<String, String> ctrlMsg = (Map<String, String>) input.getValue(3); //控制信息
        //retrieval任务,一个query视频
        if (Const.STORM_CONFIG.RETRIEVAL_TASK_FLAG.equals(taskType)) {
            String videoInfoStr = ctrlMsg.get("queryVideo");
            VideoInfoEntity videoInfoEnt = (new Gson()).fromJson(videoInfoStr,
                VideoInfoEntity.class);
            List<KeyFrameEntity> keyframeList = (new KeyFrameDao())
                .getKeyFrameByVideoId(videoInfoEnt.getVideoId());
            //如果该视频没有对应的关键帧信息,则将全局标签设为null并输出
            if (null == keyframeList || keyframeList.isEmpty()) {
                //在控制信息中加入新字段
                ctrlMsg.put("globalSignature", null);
                //移除不必要的key
                if (Const.SSM_CONFIG.IS_REDUCTIION) {
                    ctrlMsg = Const.SSM_CONFIG.discardInvalidKey("GlobalFeatureBolt",
                        ctrlMsg);
                }
                //以duration做fieldGrouping,一个bolt负责的时长范围为Const.STORM_CONFIG.BOLT_DURATION_WINDOW
                collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
                return;
            }
            Collections.sort(keyframeList, new KeyFrameEntity());
            String keyframeListStr = (new Gson()).toJson(keyframeList);
            ctrlMsg.put("keyframeList", keyframeListStr);
            HSVSigEntity hsvSignature = GlobalSigGenerator.generate(keyframeList);
            VideoHSVSigEntity videoHsvSig = new VideoHSVSigEntity(videoInfoEnt.getVideoId(),
                hsvSignature);
            String newGsonStr = (new Gson()).toJson(videoHsvSig);
            //在控制信息中加入新字段
            ctrlMsg.put("globalSignature", newGsonStr);
            //移除不必要的key
            if (Const.SSM_CONFIG.IS_REDUCTIION) {
                ctrlMsg = Const.SSM_CONFIG.discardInvalidKey("GlobalFeatureBolt",
                    ctrlMsg);
            }
            //以duration做fieldGrouping,一个bolt负责的时长范围为Const.STORM_CONFIG.BOLT_DURATION_WINDOW
            collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
            int msgLength = 0;
            for (Map.Entry<String, String> entry : ctrlMsg.entrySet()) {
                msgLength += entry.getKey().length() + entry.getValue().length();
            }
            logger.info("Control message size in global feature: " + msgLength + ", taskId: "
                        + taskId);
        }
        //detection任务,两个query视频
        else if (Const.STORM_CONFIG.DETECTION_TASK_FLAG.equals(taskType)) {
            String videoInfoStr = ctrlMsg.get("queryVideo");
            VideoInfoEntity videoInfoEnt = (new Gson()).fromJson(videoInfoStr,
                VideoInfoEntity.class);
            List<KeyFrameEntity> keyframeList = (new KeyFrameDao())
                .getKeyFrameByVideoId(videoInfoEnt.getVideoId());
            //如果该视频没有对应的关键帧信息,则输出null
            if (null == keyframeList || keyframeList.isEmpty()) {
                //在控制信息中加入第一个query视频的全局标签,设为null
                ctrlMsg.put("globalSignature", null);
                //在控制信息中加入第二个query视频的全局标签,设为null
                ctrlMsg.put("globalSignature2", null);
                //控制fieldGrouping,一个bolt负责一个时间段的视频,detection任务其实不需要fieldGrouping
                collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
                return;
            }
            Collections.sort(keyframeList, new KeyFrameEntity());
            String keyframeListStr = (new Gson()).toJson(keyframeList);
            ctrlMsg.put("keyframeList", keyframeListStr);
            HSVSigEntity hsvSignature = GlobalSigGenerator.generate(keyframeList);
            //如果生成全局标签失败,则输出null
            if (null == hsvSignature) {
                //在控制信息中加入第一个query视频的全局标签,设为null
                ctrlMsg.put("globalSignature", null);
                //在控制信息中加入第二个query视频的全局标签,设为null
                ctrlMsg.put("globalSignature2", null);
                //控制fieldGrouping,一个bolt负责一个时间段的视频,detection任务其实不需要fieldGrouping
                collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
                return;
            }
            VideoHSVSigEntity videoHsvSig = new VideoHSVSigEntity(videoInfoEnt.getVideoId(),
                hsvSignature);
            String newGsonStr = (new Gson()).toJson(videoHsvSig);
            //在控制信息中加入第一个query视频的全局标签
            ctrlMsg.put("globalSignature", newGsonStr);
            String videoInfoStr2 = ctrlMsg.get("queryVideo2");
            VideoInfoEntity videoInfoEnt2 = (new Gson()).fromJson(videoInfoStr2,
                VideoInfoEntity.class);
            List<KeyFrameEntity> keyframeList2 = (new KeyFrameDao())
                .getKeyFrameByVideoId(videoInfoEnt2.getVideoId());
            //如果该视频没有对应的关键帧信息,则输出null
            if (null == keyframeList2 || keyframeList2.isEmpty()) {
                //在控制信息中加入第一个query视频的全局标签,设为null
                ctrlMsg.put("globalSignature", null);
                //在控制信息中加入第二个query视频的全局标签,设为null
                ctrlMsg.put("globalSignature2", null);
                //控制fieldGrouping,一个bolt负责一个时间段的视频,detection任务其实不需要fieldGrouping
                collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
                return;
            }
            Collections.sort(keyframeList2, new KeyFrameEntity());
            String keyframeListStr2 = (new Gson()).toJson(keyframeList);
            ctrlMsg.put("keyframeList2", keyframeListStr2);
            HSVSigEntity hsvSignature2 = GlobalSigGenerator.generate(keyframeList2);
            //如果生成全局标签失败,则输出null
            if (null == hsvSignature2) {
                //在控制信息中加入第一个query视频的全局标签,设为null
                ctrlMsg.put("globalSignature", null);
                //在控制信息中加入第二个query视频的全局标签,设为null
                ctrlMsg.put("globalSignature2", null);
                //控制fieldGrouping,一个bolt负责一个时间段的视频,detection任务其实不需要fieldGrouping
                collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
                return;
            }
            VideoHSVSigEntity videoHsvSig2 = new VideoHSVSigEntity(videoInfoEnt2.getVideoId(),
                hsvSignature2);
            String newGsonStr2 = (new Gson()).toJson(videoHsvSig2);
            //在控制信息中加入第二个query视频的全局标签
            ctrlMsg.put("globalSignature2", newGsonStr2);
            //控制fieldGrouping,一个bolt负责一个时间段的视频,detection任务其实不需要fieldGrouping
            collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
        }
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {
        List<KeyFrameEntity> keyframeList = (new KeyFrameDao()).getKeyFrameByVideoId("1");
        Collections.sort(keyframeList, new KeyFrameEntity());
        String keyframeListStr = (new Gson()).toJson(keyframeList);
        System.out.println(keyframeListStr);
    }
}
