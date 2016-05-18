/**
 * @Package cn.pku.net.db.storm.ndvr.customized
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.customized;

import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.gson.Gson;

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

/**
 * Description: Customized bolt for retrieval task, generate global visual signature, send to bolt2
 *
 * @author jeremyjiang
 * Created at 2016/5/12 20:32
 */
public class CusGlobalRetriBolt1 extends BaseBasicBolt {
    private static final Logger logger = Logger.getLogger(CusGlobalRetriBolt1.class);

    /**
     * Declare output fields.
     *
     * @param declarer the declarer
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer) backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("taskId",
                                    "taskType",
                                    "queryVideo",
                                    "globalSignature",
                                    "startTimeStamp",
                                    "fieldGroupingId"));
    }

    /**
     * Execute.
     *
     * @param input     the input
     * @param collector the collector
     * @see backtype.storm.topology.IBasicBolt#execute(backtype.storm.tuple.Tuple, backtype.storm.topology.BasicOutputCollector) backtype.storm.topology.IBasicBolt#execute(backtype.storm.tuple.Tuple, backtype.storm.topology.BasicOutputCollector)
     */
    public void execute(Tuple input, BasicOutputCollector collector) {
        String               taskId          = input.getStringByField("taskId");
        String               taskType        = input.getStringByField("taskType");
        String               queryVideoStr   = input.getStringByField("queryVideo");
        long                 startTimeStamp  = input.getLongByField("startTimeStamp");
        int                  fieldGroupingId = input.getIntegerByField("fieldGroupingId");
        VideoInfoEntity      queryVideo      = (new Gson()).fromJson(queryVideoStr, VideoInfoEntity.class);
        List<KeyFrameEntity> keyframeList    = (new KeyFrameDao()).getKeyFrameByVideoId(queryVideo.getVideoId());

        // 如果该视频没有对应的关键帧信息,则将全局标签设为null并输出
        if ((null == keyframeList) || keyframeList.isEmpty()) {
            collector.emit(new Values(taskId, taskType, queryVideoStr, null, startTimeStamp, fieldGroupingId));

            return;
        }

        Collections.sort(keyframeList, new KeyFrameEntity());

        HSVSigEntity      queryHsvSignature       = GlobalSigGenerator.generate(keyframeList);
        VideoHSVSigEntity queryVideoHsvSig        = new VideoHSVSigEntity(queryVideo.getVideoId(), queryHsvSignature);
        String            queryGlobalSignatureStr = (new Gson()).toJson(queryVideoHsvSig);

        collector.emit(new Values(taskId,
                                  taskType,
                                  queryVideoStr,
                                  queryGlobalSignatureStr,
                                  startTimeStamp,
                                  fieldGroupingId));
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
