/**
 * @Title: CustomizedGlobalRetrievalBolt1.java 
 * @Package cn.pku.net.db.storm.ndvr.customized 
 * @Description: TODO
 * @author Jiawei Jiang    
 * @date 2015年2月9日 下午7:12:22 
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved.
 */
package cn.pku.net.db.storm.ndvr.customized;

import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.pku.net.db.storm.ndvr.dao.KeyFrameDao;
import cn.pku.net.db.storm.ndvr.entity.HSVSigEntity;
import cn.pku.net.db.storm.ndvr.entity.KeyFrameEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoHSVSigEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoInfoEntity;
import cn.pku.net.db.storm.ndvr.util.GlobalSigGenerator;

import com.google.gson.Gson;

/**
 * @ClassName: CustomizedGlobalRetrievalBolt1 
 * @Description: TODO
 * @author Jiawei Jiang
 * @date 2015年2月9日 下午7:12:22
 */
public class CustomizedGlobalRetrievalBolt1 extends BaseBasicBolt {

    private static final Logger logger = Logger.getLogger(CustomizedGlobalRetrievalBolt1.class);

    /** 
     * @see backtype.storm.topology.IBasicBolt#execute(backtype.storm.tuple.Tuple, backtype.storm.topology.BasicOutputCollector)
     */
    public void execute(Tuple input, BasicOutputCollector collector) {
        String taskId = input.getStringByField("taskId");
        String taskType = input.getStringByField("taskType");
        String queryVideoStr = input.getStringByField("queryVideo");
        long startTimeStamp = input.getLongByField("startTimeStamp");
        int fieldGroupingId = input.getIntegerByField("fieldGroupingId");
        VideoInfoEntity queryVideo = (new Gson()).fromJson(queryVideoStr, VideoInfoEntity.class);
        List<KeyFrameEntity> keyframeList = (new KeyFrameDao()).getKeyFrameByVideoId(queryVideo
            .getVideoId());
        //如果该视频没有对应的关键帧信息,则将全局标签设为null并输出
        if (null == keyframeList || keyframeList.isEmpty()) {
            collector.emit(new Values(taskId, taskType, queryVideoStr, null, startTimeStamp,
                fieldGroupingId));
            return;
        }
        Collections.sort(keyframeList, new KeyFrameEntity());
        HSVSigEntity queryHsvSignature = GlobalSigGenerator.generate(keyframeList);
        VideoHSVSigEntity queryVideoHsvSig = new VideoHSVSigEntity(queryVideo.getVideoId(),
            queryHsvSignature);
        String queryGlobalSignatureStr = (new Gson()).toJson(queryVideoHsvSig);
        collector.emit(new Values(taskId, taskType, queryVideoStr, queryGlobalSignatureStr,
            startTimeStamp, fieldGroupingId));
    }

    /** 
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("taskId", "taskType", "queryVideo", "globalSignature",
            "startTimeStamp", "fieldGroupingId"));
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

    }

}
