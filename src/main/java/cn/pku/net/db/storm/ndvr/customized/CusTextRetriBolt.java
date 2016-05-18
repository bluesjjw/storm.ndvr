/**
 * @Package cn.pku.net.db.storm.ndvr.customized
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.customized;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.gson.Gson;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.dao.VideoInfoDao;
import cn.pku.net.db.storm.ndvr.entity.TextSimilarVideo;
import cn.pku.net.db.storm.ndvr.entity.VideoInfoEntity;
import cn.pku.net.db.storm.ndvr.util.MyStringUtils;

/**
 * Description: Customized bolt for retrieval task, generate and compare the textual signature
 *
 * @author jeremyjiang
 * Created at 2016/5/12 20:50
 */
public class CusTextRetriBolt extends BaseBasicBolt {
    private static final Logger logger = Logger.getLogger(CusTextRetriBolt.class);

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
                                    "textSimilarVideoList",
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
        String          taskId          = input.getStringByField("taskId");
        String          taskType        = input.getStringByField("taskType");
        String          queryVideoStr   = input.getStringByField("queryVideo");
        long            startTimeStamp  = input.getLongByField("startTimeStamp");
        int             fieldGroupingId = input.getIntegerByField("fieldGroupingId");
        VideoInfoEntity queryVideo      = (new Gson()).fromJson(queryVideoStr, VideoInfoEntity.class);

        // 待比较视频的id集合(唯一集合),根据视频id即可以在数据库找到该视频的全局标签
        Set<String> comparedVideoIdSet = new HashSet<String>();

        // 待检索视频的时长
        int queryVideoDuration = queryVideo.getDuration();

        // 计算视频时长比较窗口的大小
        int videoDurationWindowMin = queryVideoDuration - Const.STORM_CONFIG.VIDEO_DURATION_WINDOW;

        if (videoDurationWindowMin <= 0) {
            videoDurationWindowMin = 1;
        }

        int videoDurationWindowMax = queryVideoDuration + Const.STORM_CONFIG.VIDEO_DURATION_WINDOW;

        for (int duration = videoDurationWindowMin; duration <= videoDurationWindowMax; duration++) {
            List<VideoInfoEntity> videoInfosByDuration = (new VideoInfoDao()).getVideoInfoByDuration(duration);
            Set<String>           videoIdSet           = new HashSet<String>();

            for (VideoInfoEntity videoInfoEnt : videoInfosByDuration) {
                videoIdSet.add(videoInfoEnt.getVideoId());
            }

            logger.info("Cache duration:" + duration + ", size:" + videoIdSet.size());

            // 存入待比较视频列表
            comparedVideoIdSet.addAll(videoIdSet);
        }

        // 输出结果,保存相似的视频
        List<TextSimilarVideo> textSimilarVideoList = new ArrayList<TextSimilarVideo>();

        // query视频的文本信息
        String queryVideoText = queryVideo.getTitle();

        // 依次比较compare视频和query视频
        for (String comparedVideoId : comparedVideoIdSet) {

            // 如果为检索视频本身，则跳过
            if (comparedVideoId.equals(queryVideo.getVideoId())) {
                continue;
            }

            // 待比较视频的文本信息
            VideoInfoEntity comparedVideoInfo = (new VideoInfoDao()).getVideoInfoById(comparedVideoId);
            String          comparedVideoText = comparedVideoInfo.getTitle();

            // 如果query或者compare视频的文本信息为空,则继续比较下个视频
            if ((null == queryVideoText) || (null == comparedVideoText)) {
                logger.info("query或者compare视频的文本信息为空: " + queryVideo.getVideoId() + " with " + comparedVideoId);

                continue;
            }

            List<String> querySplitText    = MyStringUtils.wordSegment(queryVideoText);
            List<String> comparedSplitText = MyStringUtils.wordSegment(comparedVideoText);

            // 如果query或者compare视频分词结果为空,则继续比较下个视频
            if (querySplitText.isEmpty() || comparedSplitText.isEmpty()) {
                logger.info("query或者compare视频分词结果为空: " + queryVideo.getVideoId() + " with " + comparedVideoId);

                continue;
            }

            float queryVScompared = (float) 0.0;    // query与compare逐词比较的相似度
            float comparedVSquery = (float) 0.0;    // compare与query逐词比较的相似度
            int   sameTermNum     = 0;

            // 计算query与compare相同的term数量占query总term的比例
            for (int i = 0; i < querySplitText.size(); i++) {
                int minIndex = (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) >= 0
                               ? (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW)
                               : 0;
                int maxIndex = (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) < comparedSplitText.size()
                               ? (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW)
                               : (comparedSplitText.size() - 1);

                for (int j = minIndex; j < maxIndex + 1; j++) {
                    if (querySplitText.get(i).equals(comparedSplitText.get(j))) {
                        sameTermNum++;

                        break;
                    }
                }
            }

            queryVScompared = (float) sameTermNum / (float) querySplitText.size();

            // 计算compare与query相同的term数量占compare总term的比例
            sameTermNum = 0;

            for (int i = 0; i < comparedSplitText.size(); i++) {
                int minIndex = (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) >= 0
                               ? (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW)
                               : 0;
                int maxIndex = (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) < querySplitText.size()
                               ? (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW)
                               : (querySplitText.size() - 1);

                for (int j = minIndex; j < maxIndex + 1; j++) {
                    if (comparedSplitText.get(i).equals(querySplitText.get(j))) {
                        sameTermNum++;

                        break;
                    }
                }
            }

            comparedVSquery = (float) sameTermNum / (float) comparedSplitText.size();

            // 调和相似度
            float harmonicSimilarity = queryVScompared * comparedVSquery / (queryVScompared + comparedVSquery);

            // 如果相似度大于阈值,存入相似列表
            if (harmonicSimilarity >= Const.STORM_CONFIG.TEXT_SIMILARITY_THRESHOLD) {
                TextSimilarVideo textSimilarVideo = new TextSimilarVideo(comparedVideoId, harmonicSimilarity);

                textSimilarVideoList.add(textSimilarVideo);
            }
        }

        // 按照距离从小到大进行排序
        Collections.sort(textSimilarVideoList, new TextSimilarVideo());

        // 在控制信息中加入相似视频列表,有可能为空!
        String textSimVideoListStr = (new Gson()).toJson(textSimilarVideoList);

        // bolt输出
        collector.emit(new Values(taskId,
                                  taskType,
                                  queryVideoStr,
                                  textSimVideoListStr,
                                  startTimeStamp,
                                  fieldGroupingId));
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
