/**
 * Created by jeremyjiang on 2016/6/8.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */
package cn.pku.net.db.storm.ndvr.distribute;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.customized.CusTextRetriBolt;
import cn.pku.net.db.storm.ndvr.dao.TaskResultDao;
import cn.pku.net.db.storm.ndvr.dao.VideoInfoDao;
import cn.pku.net.db.storm.ndvr.entity.TaskEntity;
import cn.pku.net.db.storm.ndvr.entity.TextSimilarVideo;
import cn.pku.net.db.storm.ndvr.entity.VideoInfoEntity;
import cn.pku.net.db.storm.ndvr.util.MyStringUtils;
import cn.pku.net.db.storm.ndvr.util.SigSim;
import com.google.gson.Gson;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Description:
 *
 * @author jeremyjiang
 *         Created at 2016/6/8 8:59
 */

public class DisTextRetri extends BaseBasicBolt {

    private static final Logger logger = Logger.getLogger(CusTextRetriBolt.class);

    /**
     * Declare output fields.
     *
     * @param declarer the declarer
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer) backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) { }

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

            // 调和相似度
            float textSimilarity = SigSim.getTextSim(queryVideoText, comparedVideoText);

            // 如果相似度大于阈值,存入相似列表
            if (textSimilarity >= Const.STORM_CONFIG.TEXT_SIMILARITY_THRESHOLD) {
                TextSimilarVideo textSimilarVideo = new TextSimilarVideo(comparedVideoId, textSimilarity);
                textSimilarVideoList.add(textSimilarVideo);
            }
        }

        // 按照距离从小到大进行排序
        Collections.sort(textSimilarVideoList, new TextSimilarVideo());

        TaskEntity task                 = new TaskEntity();
        task.setTaskId(taskId);
        task.setTaskType(taskType);

        List<String> videoIdList = new ArrayList<String>();
        if (null != textSimilarVideoList) {
            for (TextSimilarVideo similarVideo : textSimilarVideoList) {
                videoIdList.add(similarVideo.getVideoId());
            }
        }
        task.setVideoIdList(videoIdList);
        task.setStatus("1");
        task.setTimeStamp(Long.toString(System.currentTimeMillis() - startTimeStamp));
        TaskResultDao taskResultDao = new TaskResultDao();
        taskResultDao.insert(task);
    }
}