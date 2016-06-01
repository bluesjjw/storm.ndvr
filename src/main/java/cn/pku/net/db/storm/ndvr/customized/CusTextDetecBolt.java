/**
 * @Package cn.pku.net.db.storm.ndvr.customized
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.customized;

import java.util.List;

import org.apache.log4j.Logger;

import com.google.gson.Gson;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.entity.VideoInfoEntity;
import cn.pku.net.db.storm.ndvr.util.MyStringUtils;

/**
 * Description: Customized bolt for detection task, generate and compare textual signature of two videos
 *
 * @author jeremyjiang
 * Created at 2016/5/12 20:37
 */
public class CusTextDetecBolt extends BaseBasicBolt {
    private static final Logger logger = Logger.getLogger(CusTextDetecBolt.class);

    /**
     * Declare output fields.
     *
     * @param declarer the declarer
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer) backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("taskId", "taskType", "queryVideo1", "queryVideo2", "textSimilarity",
                "startTimeStamp", "fieldGroupingId"));
    }

    /**
     * Execute.
     *
     * @param input     the input
     * @param collector the collector
     * @see backtype.storm.topology.IBasicBolt#execute(backtype.storm.tuple.Tuple, backtype.storm.topology.BasicOutputCollector) backtype.storm.topology.IBasicBolt#execute(backtype.storm.tuple.Tuple, backtype.storm.topology.BasicOutputCollector)
     */
    public void execute(Tuple input, BasicOutputCollector collector) {
        String taskId         = input.getStringByField("taskId");
        String taskType       = input.getStringByField("taskType");
        String queryVideoStr1 = input.getStringByField("queryVideo1");
        String queryVideoStr2 = input.getStringByField("queryVideo2");
        long   startTimeStamp = input.getLongByField("startTimeStamp");
        int   fieldGroupingId = input.getIntegerByField("fieldGroupingId");

        // long startTimeStamp = System.currentTimeMillis();
        VideoInfoEntity queryVideo1 = (new Gson()).fromJson(queryVideoStr1, VideoInfoEntity.class);
        VideoInfoEntity queryVideo2 = (new Gson()).fromJson(queryVideoStr2, VideoInfoEntity.class);

        float textSimilarity = (float) 0.0;

        // 如果两个视频duration相差太大,则文本相似度设为0,输出tuple
        if (Math.abs(queryVideo1.getDuration() - queryVideo2.getDuration())
                > Const.STORM_CONFIG.VIDEO_DURATION_WINDOW) {
            collector.emit(new Values(taskId, taskType, queryVideoStr1, queryVideoStr2, textSimilarity, startTimeStamp, fieldGroupingId));
            return;
        }

        String queryVideoText1 = queryVideo1.getTitle();
        String queryVideoText2 = queryVideo2.getTitle();

        // 如果两个query视频的文本信息为空,则文本相似度设为0,输出tuple
        if ((null == queryVideoText1) || (null == queryVideoText2)) {
            collector.emit(new Values(taskId, taskType, queryVideoStr1, queryVideoStr2, textSimilarity, startTimeStamp, fieldGroupingId));
            return;
        }

        List<String> querySplitText1 = MyStringUtils.wordSegment(queryVideoText1);
        List<String> querySplitText2 = MyStringUtils.wordSegment(queryVideoText2);

        // 如果两个query视频分词结果为空,则文本相似度设为0,输出tuple
        if (querySplitText1.isEmpty() || querySplitText2.isEmpty()) {
            collector.emit(new Values(taskId, taskType, queryVideoStr1, queryVideoStr2, textSimilarity, startTimeStamp, fieldGroupingId));
            return;
        }

        // 计算query与compare相同的term数量占query总term的比例
        float query1VS2   = (float) 0.0;    // query1与query2逐词比较的相似度
        int   sameTermNum = 0;
        for (int i = 0; i < querySplitText1.size(); i++) {
            int minIndex = (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) >= 0
                           ? (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) : 0;
            int maxIndex = (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) < querySplitText2.size()
                           ? (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) : (querySplitText2.size() - 1);

            for (int j = minIndex; j < maxIndex + 1; j++) {
                if (querySplitText1.get(i).equals(querySplitText2.get(j))) {
                    sameTermNum++;
                    break;
                }
            }
        }
        query1VS2 = sameTermNum / (float) querySplitText1.size();

        // 计算compare与query相同的term数量占compare总term的比例
        float query2VS1   = (float) 0.0;    // query2与query1逐词比较的相似度
        sameTermNum = 0;
        for (int i = 0; i < querySplitText2.size(); i++) {
            int minIndex = (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) >= 0
                           ? (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) : 0;
            int maxIndex = (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) < querySplitText1.size()
                           ? (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) : (querySplitText1.size() - 1);
            for (int j = minIndex; j < maxIndex + 1; j++) {
                if (querySplitText2.get(i).equals(querySplitText1.get(j))) {
                    sameTermNum++;
                    break;
                }
            }
        }
        query2VS1 = sameTermNum / (float) querySplitText2.size();

        // 调和相似度
        textSimilarity = (float) 0.0;

        if ((query1VS2 == 0) || (query2VS1 == 0)) {
            textSimilarity = 0;
        } else {
            textSimilarity = query1VS2 * query2VS1 / (query1VS2 + query2VS1);
        }

        // 放入文本相似度,输出
        collector.emit(new Values(taskId, taskType, queryVideoStr1, queryVideoStr2, textSimilarity, startTimeStamp, fieldGroupingId));
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
