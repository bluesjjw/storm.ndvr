/**
 * @Package cn.pku.net.db.storm.ndvr.bolt
 * Created by jeremyjiang on 2016/5/13.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.general;

import java.lang.reflect.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import cn.pku.net.db.storm.ndvr.dao.KeyFrameDao;
import cn.pku.net.db.storm.ndvr.entity.*;
import cn.pku.net.db.storm.ndvr.util.LocalSigGenerator;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.dao.VideoInfoDao;
import cn.pku.net.db.storm.ndvr.image.analyze.sift.match.Match;
import cn.pku.net.db.storm.ndvr.image.analyze.sift.match.MatchKeys;
import cn.pku.net.db.storm.ndvr.image.analyze.sift.scale.KDFeaturePoint;

import static cn.pku.net.db.storm.ndvr.general.StreamSharedMessage.discardInvalidKey;

/**
 * Description: General bolt, compare local visual signature
 *
 * @author jeremyjiang
 * Created at 2016/5/13 9:43
 */
public class LocalSigSimilarBolt extends BaseBasicBolt {
    private static final Logger       logger                  = Logger.getLogger(LocalSigSimilarBolt.class);
    private Map<Integer, Set<String>> cachedVideoIdByDuration = new ConcurrentHashMap<Integer, Set<String>>();    // 缓存视频数据,key为duration,value为视频id的列表
    private static Map<String, List<KeyFrameEntity>> cachedKeyFrame = new ConcurrentHashMap<String, List<KeyFrameEntity>>();   // 缓存的视频关键帧
    private static Map<String, List<SIFTSigEntity>> cachedSIFTSigs = new ConcurrentHashMap<String, List<SIFTSigEntity>>();   // 缓存的SIFT标签
    private static Map<String, Float> cachedSimilarity = new ConcurrentHashMap<String, Float>();

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
                List<SIFTSigEntity> siftSig = LocalSigGenerator.getFromFile(videoInfoEnt.getVideoId());
                if (null != siftSig && !siftSig.isEmpty()){
                    cachedSIFTSigs.put(videoInfoEnt.getVideoId(), siftSig);
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
     * @see backtype.storm.topology.IBasicBolt#execute(backtype.storm.tuple.Tuple,
     * backtype.storm.topology.BasicOutputCollector) backtype.storm.topology.IBasicBolt#execute(backtype.storm.tuple.Tuple,
     * backtype.storm.topology.BasicOutputCollector)
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String taskId = input.getStringByField("taskId");
        logger.info("Local signature distance, taskId: " + taskId);
        String              taskType        = input.getStringByField("taskType");
        int                 fieldGroupingId = input.getIntegerByField("fieldGroupingId");
        Map<String, String> ctrlMsg         = (Map<String, String>) input.getValue(3);    // 控制信息

        // retrieval任务,一个query视频
        if (Const.STORM_CONFIG.RETRIEVAL_TASK_FLAG.equals(taskType)) {
            // query视频
            String          queryVideoInfoStr = ctrlMsg.get("queryVideo");
            VideoInfoEntity queryVideoInfo    = (new Gson()).fromJson(queryVideoInfoStr, VideoInfoEntity.class);

            // 待比较视频的id集合(唯一集合),根据视频id即可以找到该视频的SIFT标签文件
            Set<String> comparedVideoIdSet = new HashSet<String>();

            // 如果采用filter-and-refine的策略,则将之前全局特征处理之后的相似视频作为比较集
            if (Const.STORM_CONFIG.IS_FILTER_AND_REFINE && ctrlMsg.containsKey("globalSimilarVideoList")) {
                // 解析全局特征相似的视频列表
                String                   globalSimilarVideoListStr  = ctrlMsg.get("globalSimilarVideoList");
                Type                     globalSimilarVideoListType = new TypeToken<List<GlobalSimilarVideo>>(){}.getType();
                List<GlobalSimilarVideo> globalSimilarVideoList = (new Gson()).fromJson(globalSimilarVideoListStr, globalSimilarVideoListType);
                for (GlobalSimilarVideo globalSimilarVideo : globalSimilarVideoList) {
                    comparedVideoIdSet.add(globalSimilarVideo.getVideoId());
                }
            }
            // 如果采用filter-and-refine的策略,没有全局特征,则将之前文本相似度处理之后的相似视频作为比较集
            else if (Const.STORM_CONFIG.IS_FILTER_AND_REFINE && ctrlMsg.containsKey("textSimilarVideoList")) {
                // 解析文本内容相似的视频列表
                String                 textSimilarVideoListStr  = ctrlMsg.get("textSimilarVideoList");
                Type                   textSimilarVideoListType = new TypeToken<List<TextSimilarVideo>>() {}.getType();
                List<TextSimilarVideo> textSimilarVideoList = (new Gson()).fromJson(textSimilarVideoListStr, textSimilarVideoListType);
                for (TextSimilarVideo textSimilarVideo : textSimilarVideoList) {
                    comparedVideoIdSet.add(textSimilarVideo.getVideoId());
                }
            }
            // 如果不采用filter-and-refine的策略,则根据视频时长选取合适的视频
            else {
                // 待检索视频的时长
                int queryVideoDuration = queryVideoInfo.getDuration();
                // 计算视频时长比较窗口的大小
                int videoDurationWindowMin = queryVideoDuration - Const.STORM_CONFIG.VIDEO_DURATION_WINDOW;
                if (videoDurationWindowMin <= 0) {
                    videoDurationWindowMin = 1;
                }
                int videoDurationWindowMax = queryVideoDuration + Const.STORM_CONFIG.VIDEO_DURATION_WINDOW;

                for (int duration = videoDurationWindowMin; duration <= videoDurationWindowMax; duration++) {
                    // 如果cache中没有对应时长的视频,则查询数据库
                    if (!this.cachedVideoIdByDuration.containsKey(duration)) {
                        List<VideoInfoEntity> videoInfosByDuration =
                            (new VideoInfoDao()).getVideoInfoByDuration(duration);
                        Set<String> videoIdSet = new HashSet<String>();
                        for (VideoInfoEntity videoInfoEnt : videoInfosByDuration) {
                            videoIdSet.add(videoInfoEnt.getVideoId());
                        }
                        // 存入cache
                        this.cachedVideoIdByDuration.put(duration, videoIdSet);
                        // 存入待比较视频列表
                        comparedVideoIdSet.addAll(videoIdSet);
                    }
                    // 如果cache中有对应时长的视频,则直接查询内存的Map
                    else {
                        comparedVideoIdSet.addAll(this.cachedVideoIdByDuration.get(duration));
                    }
                }
            }

            // 获得query视频的局部标签
            String queryLocalSigStr = ctrlMsg.get("localSignature");
            // query视频SIFT标签的type
            Type queryLocalSigType = new TypeToken<List<List<KDFeaturePoint>>>(){ }.getType();
            // query视频的标签,list大小为query视频帧图像的数量,内层list大小为SIFT特征点的个数
            List<List<KDFeaturePoint>> queryLocalSigs = (new Gson()).fromJson(queryLocalSigStr, queryLocalSigType);

            // 保存局部特征相似的视频
            List<LocalSimilarVideo> localSimilarVideoList = new ArrayList<LocalSimilarVideo>();

            // 如果没有局部标签,则输出空列表
            if ((null == queryLocalSigs) || queryLocalSigs.isEmpty()) {
                ctrlMsg.put("localSimilarVideoList", (new Gson()).toJson(localSimilarVideoList));
                // 移除不必要的key
                ctrlMsg = StreamSharedMessage.discardInvalidKey("LocalSigSimilarBolt", ctrlMsg);
                // bolt输出
                collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
                return;
            }

            // 依次和视频时长在窗口内的视频比较
            for (String comparedVideoId : comparedVideoIdSet) {
                // 如果为检索视频本身,则跳过
                if (comparedVideoId.equals(queryVideoInfo.getVideoId())) {
                    continue;
                }
                float localSigSimilarity = (float) 0.0;
                if (cachedSimilarity.containsKey(queryVideoInfo.getVideoId() + comparedVideoId)){
                    localSigSimilarity = cachedSimilarity.get(queryVideoInfo.getVideoId() + comparedVideoId);
                } else {
                    List<SIFTSigEntity> comparedKeyframeSigs = null;
                    if (!cachedSIFTSigs.containsKey(comparedVideoId)) {
                        comparedKeyframeSigs = LocalSigGenerator.getFromFile(comparedVideoId);
                        if (null != comparedKeyframeSigs && !comparedKeyframeSigs.isEmpty()) {
                            cachedSIFTSigs.put(comparedVideoId, comparedKeyframeSigs);
                        }
                    } else {
                        comparedKeyframeSigs = getSIFTSigFromCache(comparedVideoId);
                    }

                    // 记录相似的帧图像数量
                    int similarKeyframeNum = 0;
                    if (null != comparedKeyframeSigs && !comparedKeyframeSigs.isEmpty()) {
                        // i表示query视频的帧序号,依次将query视频的每个帧与compare视频进行比较
                        for (int i = 0; i < queryLocalSigs.size(); i++) {
                            // compare视频帧图像的比较窗口边界
                            int comparedFrameLeft = i - Const.STORM_CONFIG.FRAME_COMPARED_WINDOW;
                            if (comparedFrameLeft < 0) {
                                comparedFrameLeft = 0;
                            }
                            int comparedFrameRight = i + Const.STORM_CONFIG.FRAME_COMPARED_WINDOW;
                            if (comparedFrameRight >= comparedKeyframeSigs.size()) {
                                comparedFrameRight = comparedKeyframeSigs.size() - 1;
                            }
                            // j表示compare视频的帧序号
                            for (int j = comparedFrameLeft; j <= comparedFrameRight; j++) {
                                if (queryLocalSigs.get(i).isEmpty() || comparedKeyframeSigs.get(j).getSig().isEmpty()) {
                                    continue;
                                }
                                try {
                                    // 比较query视频和compare视频
                                    List<Match> ms = MatchKeys.findMatchesBBF(queryLocalSigs.get(i), comparedKeyframeSigs.get(j).getSig());
                                    ms = MatchKeys.filterMore(ms);
                                    // 如果找到一个相似度大于阈值的帧,则开始比较query视频的下一个帧图像
                                    if ((float) ms.size() / (float) queryLocalSigs.get(i).size()
                                            >= Const.STORM_CONFIG.LOCALSIG_KEYFRAME_SIMILARITY_THRESHOLD) {
                                        similarKeyframeNum++;
                                        break;
                                    }
                                } catch (IllegalArgumentException e) {
                                    logger.error("ComparedVideoId: " + comparedVideoId + ", " + e);
                                }
                            }
                        }
                    }
                    localSigSimilarity = (float) similarKeyframeNum / (float) queryLocalSigs.size();
                    cachedSimilarity.put(queryVideoInfo.getVideoId() + comparedVideoId, localSigSimilarity);
                }
                if (localSigSimilarity >= Const.STORM_CONFIG.LOCALSIG_VIDEO_SIMILARITY_THRESHOLd) {
                    LocalSimilarVideo localSimilarVideo = new LocalSimilarVideo(comparedVideoId, localSigSimilarity);
                    localSimilarVideoList.add(localSimilarVideo);
                }
            }
            Collections.sort(localSimilarVideoList, new LocalSimilarVideo());
            logger.info("Local similar video size: " + localSimilarVideoList.size() + ", taskId: " + taskId);
            // 在控制信息中加入局部特征相似视频列表,有可能为空!
            ctrlMsg.put("localSimilarVideoList", (new Gson()).toJson(localSimilarVideoList));
            // 移除不必要的key
            ctrlMsg = discardInvalidKey("LocalSigSimilarBolt", ctrlMsg);
            // bolt输出
            collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
            logger.info(String.format("Control message size in local feature distance: %d, taskId: %s",
                    StreamSharedMessage.calMsgLength(ctrlMsg), taskId));
        } else if (Const.STORM_CONFIG.DETECTION_TASK_FLAG.equals(taskType)) {
            // 视频SIFT标签的type
            Type queryLocalSigType = new TypeToken<List<List<KDFeaturePoint>>>() { }.getType();
            // 第一个视频的信息、局部标签
            String          queryVideoInfoStr1 = ctrlMsg.get("queryVideo1");
            VideoInfoEntity queryVideoInfo1    = (new Gson()).fromJson(queryVideoInfoStr1, VideoInfoEntity.class);
            String queryLocalSigStr1 = ctrlMsg.get("localSignature1");
            // 第一个视频的标签,list大小为query视频帧图像的数量,内层list大小为SIFT特征点的个数
            List<List<KDFeaturePoint>> queryLocalSigs1 = (new Gson()).fromJson(queryLocalSigStr1, queryLocalSigType);

            // 第二个视频的信息、局部标签
            String          queryVideoInfoStr2 = ctrlMsg.get("queryVideo2");
            VideoInfoEntity queryVideoInfo2    = (new Gson()).fromJson(queryVideoInfoStr2, VideoInfoEntity.class);
            String queryLocalSigStr2 = ctrlMsg.get("localSignature2");
            // 第二个视频的标签,list大小为视频帧图像的数量,内层list大小为SIFT特征点的个数
            List<List<KDFeaturePoint>> queryLocalSigs2 = (new Gson()).fromJson(queryLocalSigStr2, queryLocalSigType);

            float localSimilarity = (float) 0.0; // 0: dissimilar, 1: similar

            // 如果没有局部标签,则相似度设为0并输出
            if ((null == queryLocalSigs1) || queryLocalSigs1.isEmpty() || (null == queryLocalSigs2) || queryLocalSigs2.isEmpty()) {
                ctrlMsg.put("localSimilarity", Float.toString((float) 0.0));
                // 移除不必要的key
                    ctrlMsg = discardInvalidKey("LocalSigSimilarBolt", ctrlMsg);
                // bolt输出
                collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
                return;
            }

            // 记录相似的帧图像数量
            int similarKeyframeNum = 0;
            // i表示第一个视频的帧序号,依次将第一个视频的每个帧与第二个视频进行比较
            for (int i = 0; i < queryLocalSigs1.size(); i++) {
                // compare视频帧图像的比较窗口边界
                int comparedFrameLeft = i - Const.STORM_CONFIG.FRAME_COMPARED_WINDOW;
                if (comparedFrameLeft < 0) {
                    comparedFrameLeft = 0;
                }
                int comparedFrameRight = i + Const.STORM_CONFIG.FRAME_COMPARED_WINDOW;
                if (comparedFrameRight >= queryLocalSigs2.size()) {
                    comparedFrameRight = queryLocalSigs2.size() - 1;
                }
                // j表示第二个视频的帧序号
                for (int j = comparedFrameLeft; j <= comparedFrameRight; j++) {
                    if (queryLocalSigs1.get(i).isEmpty() || queryLocalSigs2.get(j).isEmpty()) {
                        continue;
                    }
                    try {
                        // 比较query视频和compare视频
                        List<Match> ms = MatchKeys.findMatchesBBF(queryLocalSigs1.get(i), queryLocalSigs2.get(j));
                        ms = MatchKeys.filterMore(ms);
                        // 如果找到一个相似度大于阈值的帧,则开始比较query视频的下一个帧图像
                        if ((float) ms.size() / (float) queryLocalSigs1.size() >= Const.STORM_CONFIG.LOCALSIG_KEYFRAME_SIMILARITY_THRESHOLD) {
                            similarKeyframeNum++;
                            break;
                        }
                    } catch (IllegalArgumentException e) {
                        logger.error("QueryVideoId: " + queryVideoInfo1.getVideoId() + " vs " + queryVideoInfo2.getVideoId() + ". " + e);
                    }
                }
            }

            localSimilarity = (float) similarKeyframeNum / (float) queryLocalSigs1.size();

            // 在控制信息中加入局部特征的相似度
            ctrlMsg.put("localSimilarity", Float.toString(localSimilarity));
            // 移除不必要的key
            ctrlMsg = discardInvalidKey("LocalSigSimilarBolt", ctrlMsg);
            // bolt输出
            collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
            logger.info(String.format("Control message size in local feature distance: %d, taskId: %s",
                    StreamSharedMessage.calMsgLength(ctrlMsg), taskId));
        }
    }

    private List<SIFTSigEntity> getSIFTSigFromCache (String videoId) {
        List<SIFTSigEntity> siftSig = null;
        if (!cachedSIFTSigs.containsKey(videoId)){
            siftSig = LocalSigGenerator.getFromFile(videoId);
            if (null != siftSig && !siftSig.isEmpty()) {
                cachedSIFTSigs.put(videoId, siftSig);
            }
        } else {
            siftSig = cachedSIFTSigs.get(videoId);
        }
        return siftSig;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
