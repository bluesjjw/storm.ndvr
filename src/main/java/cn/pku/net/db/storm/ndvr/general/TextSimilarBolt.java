/**
 * @Package cn.pku.net.db.storm.ndvr.bolt
 * Created by jeremyjiang on 2016/5/13.
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
import cn.pku.net.db.storm.ndvr.dao.VideoInfoDao;
import cn.pku.net.db.storm.ndvr.entity.TextSimilarVideo;
import cn.pku.net.db.storm.ndvr.entity.VideoInfoEntity;
import cn.pku.net.db.storm.ndvr.util.MyStringUtils;
import com.google.gson.Gson;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Description: General bolt, compare textual signature
 *
 * @author jeremyjiang
 * Created at 2016/5/13 9:45
 */
public class TextSimilarBolt extends BaseBasicBolt {

    private static final Logger       logger                  = Logger.getLogger(TextSimilarBolt.class);
    private Map<Integer, Set<String>> cachedVideoIdByDuration = new ConcurrentHashMap<Integer, Set<String>>();    // 缓存视频数据,key为duration,value为视频元数据
    private Map<String, String> cachedVideoText = new ConcurrentHashMap<String, String>();    // 缓存视频数据,key为视频id,value为视频文本信息

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
        VideoInfoDao          videoInfoDao  = new VideoInfoDao();
        List<VideoInfoEntity> videoInfoList = videoInfoDao.getVideoInfoByDuration(0);    // 取出时长为0的视频(数据集中有些视频没有duration数据,我们设为0)
        if ((null != videoInfoList) && !videoInfoList.isEmpty()) {
            Set<String> videoIdSet = new HashSet<String>();
            for (VideoInfoEntity videoInfoEnt : videoInfoList) {
                if (null != videoInfoEnt.getTitle()) {
                    videoIdSet.add(videoInfoEnt.getVideoId());
                    this.cachedVideoText.put(videoInfoEnt.getVideoId(), videoInfoEnt.getTitle());    // 缓存时长为0的视频的HSV全局标签
                }
            }
            this.cachedVideoIdByDuration.put(0, videoIdSet);    // 将时长为0的视频缓存
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
        //logger.info("Text similarity, taskId: " + taskId);
        String              taskType        = input.getStringByField("taskType");
        int                 fieldGroupingId = input.getIntegerByField("fieldGroupingId");
        Map<String, String> ctrlMsg         = (Map<String, String>) input.getValue(3);    // 控制信息
        long startTime = System.currentTimeMillis();

        // retrieval任务,一个query视频
        if (Const.STORM_CONFIG.RETRIEVAL_TASK_FLAG.equals(taskType)) {
            String          queryVideoInfoStr = ctrlMsg.get("queryVideo");
            VideoInfoEntity queryVideoInfo    = (new Gson()).fromJson(queryVideoInfoStr, VideoInfoEntity.class);

            // 待比较视频的id集合(唯一集合),根据视频id即可以在数据库找到该视频的全局标签
            Set<String> comparedVideoIdSet = new HashSet<String>();

            // 待检索视频的时长
            int queryVideoDuration = queryVideoInfo.getDuration();
            // 计算视频时长比较窗口的大小
            int videoDurationWindowMin = queryVideoDuration - Const.STORM_CONFIG.VIDEO_DURATION_WINDOW;
            if (videoDurationWindowMin <= 0) {
                videoDurationWindowMin = 1;
            }
            int videoDurationWindowMax = queryVideoDuration + Const.STORM_CONFIG.VIDEO_DURATION_WINDOW;

            for (int duration = videoDurationWindowMin; duration <= videoDurationWindowMax; duration++) {
                if (!this.cachedVideoIdByDuration.containsKey(duration)) {  // 如果cache中没有对应时长的视频,则查询数据库
                    List<VideoInfoEntity> videoInfosByDuration = (new VideoInfoDao()).getVideoInfoByDuration(duration);
                    Set<String>           videoIdSet           = new HashSet<String>();
                    for (VideoInfoEntity videoInfoEnt : videoInfosByDuration) {
                        videoIdSet.add(videoInfoEnt.getVideoId());
                        // 将视频文本信息存入cache
                        if (!this.cachedVideoText.containsKey(videoInfoEnt.getVideoId())) {
                            this.cachedVideoText.put(videoInfoEnt.getVideoId(), videoInfoEnt.getTitle());
                        }
                    }
                    // 存入cache
                    this.cachedVideoIdByDuration.put(duration, videoIdSet);
                    logger.info("Cache duration:" + duration + ", size:" + videoIdSet.size());
                    // 存入待比较视频列表
                    comparedVideoIdSet.addAll(videoIdSet);
                }
                else {  // 如果cache中有对应时长的视频,则直接查询内存的Map
                    comparedVideoIdSet.addAll(this.cachedVideoIdByDuration.get(duration));
                }
            }

            // 输出结果,保存相似的视频
            List<TextSimilarVideo> textSimilarVideoList = new ArrayList<TextSimilarVideo>();
            // query视频的文本信息
            String queryVideoText = queryVideoInfo.getTitle();
            // 依次比较compare视频和query视频
            for (String comparedVideoId : comparedVideoIdSet) {
                // 如果为检索视频本身，则跳过
                if (comparedVideoId.equals(queryVideoInfo.getVideoId())) {
                    continue;
                }
                // 待比较视频的文本信息
                String comparedVideoText = null;
                // 如果缓存中没有compare视频的文本信息,则将新查询到的视频标签存入缓存
                if (!cachedVideoText.containsKey(comparedVideoId)) {
                    VideoInfoEntity videoInfo = (new VideoInfoDao()).getVideoInfoById(comparedVideoId);
                    this.cachedVideoText.put(comparedVideoId, videoInfo.getTitle());
                    comparedVideoText = videoInfo.getTitle();
                }
                // 如果缓存里有compare视频的文本信息,则查询缓存
                else {
                    comparedVideoText = this.cachedVideoText.get(comparedVideoId);
                }

                // 如果query或者compare视频的文本信息为空,则继续比较下个视频
                if ((null == queryVideoText) || (null == comparedVideoText)) {
                    logger.info("query或者compare视频的文本信息为空: " + queryVideoInfo.getVideoId() + " with " + comparedVideoId);
                    continue;
                }

                List<String> querySplitText    = MyStringUtils.wordSegment(queryVideoText);
                List<String> comparedSplitText = MyStringUtils.wordSegment(comparedVideoText);
                // 如果query或者compare视频分词结果为空,则继续比较下个视频
                if (querySplitText.isEmpty() || comparedSplitText.isEmpty()) {
                    logger.info("query或者compare视频分词结果为空: " + queryVideoInfo.getVideoId() + " with " + comparedVideoId);
                    continue;
                }

                // 计算query与compare相同的term数量占query总term的比例
                float queryVScompared = (float) 0.0;    // query与compare逐词比较的相似度
                int   sameTermNum     = 0;
                for (int i = 0; i < querySplitText.size(); i++) {
                    int minIndex = (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) >= 0
                                   ? (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) : 0;
                    int maxIndex = (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) < comparedSplitText.size()
                                   ? (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) : (comparedSplitText.size() - 1);
                    for (int j = minIndex; j < maxIndex + 1; j++) {
                        if (querySplitText.get(i).equals(comparedSplitText.get(j))) {
                            sameTermNum++;
                            break;
                        }
                    }
                }
                queryVScompared = (float) sameTermNum / (float) querySplitText.size();

                // 计算compare与query相同的term数量占compare总term的比例
                float comparedVSquery = (float) 0.0;    // compare与query逐词比较的相似度
                sameTermNum = 0;
                for (int i = 0; i < comparedSplitText.size(); i++) {
                    int minIndex = (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) >= 0
                                   ? (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) : 0;
                    int maxIndex = (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) < querySplitText.size()
                                   ? (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) : (querySplitText.size() - 1);
                    for (int j = minIndex; j < maxIndex + 1; j++) {
                        if (comparedSplitText.get(i).equals(querySplitText.get(j))) {
                            sameTermNum++;
                            break;
                        }
                    }
                }
                comparedVSquery = (float) sameTermNum / (float) comparedSplitText.size();

                // 调和相似度
                float textSimilarity = (float) 0.0;
                if ((queryVScompared == 0) || (comparedVSquery == 0)) {
                    textSimilarity = 0;
                } else {
                    textSimilarity = queryVScompared * comparedVSquery / (queryVScompared + comparedVSquery);
                }
                // 如果相似度大于阈值,存入相似列表
                if (textSimilarity >= Const.STORM_CONFIG.TEXT_SIMILARITY_THRESHOLD) {
                    TextSimilarVideo textSimilarVideo = new TextSimilarVideo(comparedVideoId, textSimilarity);
                    textSimilarVideoList.add(textSimilarVideo);
                }
            }

            // 按照距离从小到大进行排序
            Collections.sort(textSimilarVideoList, new TextSimilarVideo());
            logger.info("Text similar video size:" + textSimilarVideoList.size() + ", taskId: " + taskId);
            // 在控制信息中加入相似视频列表,有可能为空!
            ctrlMsg.put("textSimilarVideoList", (new Gson()).toJson(textSimilarVideoList));
            // 移除不必要的key
            ctrlMsg = StreamSharedMessage.discardInvalidKey("TextSimilarBolt", ctrlMsg);
            // time cost in this bolt
            logger.info(String.format("TextSimilarBolt cost %d ms", (System.currentTimeMillis() - startTime)));
            // bolt输出, 监控SSM大小
            collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
        }
        // detection task,两个视频,比较它们的相似度
        else if (Const.STORM_CONFIG.DETECTION_TASK_FLAG.equals(taskType)) {
            // 获得控制信息中第一个视频的元数据
            String          queryVideoInfoStr1 = ctrlMsg.get("queryVideo1");
            VideoInfoEntity queryVideoInfo1    = (new Gson()).fromJson(queryVideoInfoStr1, VideoInfoEntity.class);
            String          queryVideoText1    = queryVideoInfo1.getTitle();
            // 获得控制信息中第二个视频的元数据
            String          queryVideoInfoStr2 = ctrlMsg.get("queryVideo2");
            VideoInfoEntity queryVideoInfo2    = (new Gson()).fromJson(queryVideoInfoStr2, VideoInfoEntity.class);
            String          queryVideoText2    = queryVideoInfo2.getTitle();

            float textSimilarity = (float) 0.0; // 0-1

            // 如果两个query视频的时长相差很大或者文本信息为空,则文本相似度设为0,输出tuple
            if (Math.abs(queryVideoInfo1.getDuration() - queryVideoInfo2.getDuration())
                    > Const.STORM_CONFIG.VIDEO_DURATION_WINDOW || null == queryVideoText1 || null == queryVideoText2) {
                ctrlMsg.put("textSimilarity", Float.toString(textSimilarity));
                // 移除不必要的key
                ctrlMsg = StreamSharedMessage.discardInvalidKey("TextSimilarBolt", ctrlMsg);
                collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
                return;
            }

            List<String> query1SplitText  = MyStringUtils.wordSegment(queryVideoText1);
            List<String> query2SplitText = MyStringUtils.wordSegment(queryVideoText2);
            // 如果两个query视频分词结果为空,则文本相似度设为0,输出tuple
            if (query1SplitText.isEmpty() || query2SplitText.isEmpty()) {
                ctrlMsg.put("textSimilarity", Float.toString(textSimilarity));
                // 移除不必要的key
                ctrlMsg = StreamSharedMessage.discardInvalidKey("TextSimilarBolt", ctrlMsg);
                collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
                return;
            }

            // 计算query与compare相同的term数量占query总term的比例
            float query1VS2   = (float) 0.0;    // query1与query2逐词比较的相似度
            int   sameTermNum = 0;
            for (int i = 0; i < query1SplitText.size(); i++) {
                int minIndex = (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) >= 0
                               ? (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) : 0;
                int maxIndex = (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) < query2SplitText.size()
                               ? (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) : (query2SplitText.size() - 1);
                for (int j = minIndex; j < maxIndex + 1; j++) {
                    if (query1SplitText.get(i).equals(query2SplitText.get(j))) {
                        sameTermNum++;
                        break;
                    }
                }
            }
            query1VS2 = (float) sameTermNum / (float) query1SplitText.size();

            // 计算compare与query相同的term数量占compare总term的比例
            float query2VS1   = (float) 0.0;    // query2与query1逐词比较的相似度
            sameTermNum = 0;
            for (int i = 0; i < query2SplitText.size(); i++) {
                int minIndex = (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) >= 0
                               ? (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) : 0;
                int maxIndex = (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) < query1SplitText.size()
                               ? (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) : (query1SplitText.size() - 1);
                for (int j = minIndex; j < maxIndex + 1; j++) {
                    if (query2SplitText.get(i).equals(query1SplitText.get(j))) {
                        sameTermNum++;
                        break;
                    }
                }
            }
            query2VS1 = (float) sameTermNum / (float) query2SplitText.size();

            if (query1VS2 != 0 && query2VS1 != 0) {
                textSimilarity = query1VS2 * query2VS1 / (query1VS2 + query2VS1);
            }
            // 放入文本相似度,输出
            ctrlMsg.put("textSimilarity", Float.toString(textSimilarity));
            // 移除不必要的key
            ctrlMsg = StreamSharedMessage.discardInvalidKey("TextSimilarBolt", ctrlMsg);
            // bolt输出, 监控SSM大小
            collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
            //logger.info(String.format("Control message size in textual feature distance: %d, taskId: %s",
            //        StreamSharedMessage.calMsgLength(ctrlMsg), taskId));
        }
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
