/**
 * Created by jeremyjiang on 2016/5/18.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */
package cn.pku.net.db.storm.ndvr.customized;

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
import cn.pku.net.db.storm.ndvr.entity.VideoInfoEntity;
import cn.pku.net.db.storm.ndvr.util.GlobalSigGenerator;
import cn.pku.net.db.storm.ndvr.util.SigSim;
import com.google.gson.Gson;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Description: Customized bolt for post-filtering detection task using textual and global visual signatures
 * this bolt generates and compares global visual signatures
 * @author jeremyjiang
 * Created at 2016/5/18 9:24
 */

public class CusTextGlobalPostDetecBolt2 extends BaseBasicBolt {
    private static final Logger logger                  = Logger.getLogger(CusTextGlobalPostDetecBolt2.class);
    private static Map<Integer, Set<String>> cachedVideoIdByDuration = new ConcurrentHashMap<Integer, Set<String>>();    // 缓存视频数据,key为duration,value为视频元数据
    private static Map<String, HSVSigEntity> cachedHSVSignature = new ConcurrentHashMap<String, HSVSigEntity>();    // 缓存视频的HSV全局标签,key为视频id,value为视频HSV全局标签

    /**
     * Declare output fields.
     *
     * @param declarer the declarer
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer) backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("taskId", "taskType", "queryVideo1", "queryVideo2", "globalDistance",
                "startTimeStamp", "fieldGroupingId"));
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
        String          taskId         = input.getStringByField("taskId");
        String          taskType       = input.getStringByField("taskType");
        String          queryVideoStr1 = input.getStringByField("queryVideo1");
        String          queryVideoStr2 = input.getStringByField("queryVideo2");
        long            startTimeStamp = input.getLongByField("startTimeStamp");
        int            fieldGroupingId = input.getIntegerByField("fieldGroupingId");

        VideoInfoEntity queryVideo1    = (new Gson()).fromJson(queryVideoStr1, VideoInfoEntity.class);
        VideoInfoEntity queryVideo2    = (new Gson()).fromJson(queryVideoStr2, VideoInfoEntity.class);

        // initiate enough large global signature distance
        float globalDistance = (float) (Const.STORM_CONFIG.GLOBALSIG_EUCLIDEAN_THRESHOLD * 10.0);

        List<KeyFrameEntity> keyframeList1 = (new KeyFrameDao()).getKeyFrameByVideoId(queryVideo1.getVideoId());
        List<KeyFrameEntity> keyframeList2 = (new KeyFrameDao()).getKeyFrameByVideoId(queryVideo2.getVideoId());
        // if keyframes do not exist, return considerable large global distance
        if (null == keyframeList1 || keyframeList1.isEmpty() || null == keyframeList2 || keyframeList2.isEmpty()) {
            collector.emit(new Values(taskId, taskType, queryVideoStr1, queryVideoStr2, globalDistance, startTimeStamp, fieldGroupingId));
            return;
        }
        HSVSigEntity hsvSignature1 = GlobalSigGenerator.generate(keyframeList1);
        HSVSigEntity hsvSignature2 = GlobalSigGenerator.generate(keyframeList2);
        // if can not generate valid HSV signature, return considerable large global distance
        if (null == hsvSignature1 || null == hsvSignature2) {
            collector.emit(new Values(taskId, taskType, queryVideoStr1, queryVideoStr2, globalDistance, startTimeStamp, fieldGroupingId));
            return;
        }
        // calculate Euclidean global distance
        globalDistance = SigSim.getEuclideanDistance(hsvSignature1, hsvSignature1);
        collector.emit(new Values(taskId, taskType, queryVideoStr1, queryVideoStr2, globalDistance, startTimeStamp, fieldGroupingId));
    }
}