/**
 * @Package cn.pku.net.db.storm.ndvr.bolt
 * Created by jeremyjiang on 2016/5/13.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.general;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.lang.reflect.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
import cn.pku.net.db.storm.ndvr.entity.GlobalSimilarVideo;
import cn.pku.net.db.storm.ndvr.entity.LocalSimilarVideo;
import cn.pku.net.db.storm.ndvr.entity.SIFTSigEntity;
import cn.pku.net.db.storm.ndvr.entity.TextSimilarVideo;
import cn.pku.net.db.storm.ndvr.entity.VideoInfoEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoSIFTSigEntity;
import cn.pku.net.db.storm.ndvr.image.analyze.sift.match.Match;
import cn.pku.net.db.storm.ndvr.image.analyze.sift.match.MatchKeys;
import cn.pku.net.db.storm.ndvr.image.analyze.sift.scale.KDFeaturePoint;

/**
 * Description: General bolt, compare local visual signature
 *
 * @author jeremyjiang
 * Created at 2016/5/13 9:43
 */
public class LocalSigDistanceBolt extends BaseBasicBolt {
    private static final Logger       logger                  = Logger.getLogger(LocalSigDistanceBolt.class);
    private Map<Integer, Set<String>> cachedVideoIdByDuration = new ConcurrentHashMap<Integer, Set<String>>();    // 缓存视频数据,key为duration,value为视频id的列表

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
                List<GlobalSimilarVideo> globalSimilarVideoList     = new ArrayList<GlobalSimilarVideo>();
                String                   globalSimilarVideoListStr  = ctrlMsg.get("globalSimilarVideoList");
                Type                     globalSimilarVideoListType = new TypeToken<List<GlobalSimilarVideo>>() {}
                .getType();

                globalSimilarVideoList = (new Gson()).fromJson(globalSimilarVideoListStr, globalSimilarVideoListType);

                for (GlobalSimilarVideo globalSimilarVideo : globalSimilarVideoList) {
                    comparedVideoIdSet.add(globalSimilarVideo.getVideoId());
                }
            }

            // 如果采用filter-and-refine的策略,没有全局特征,则将之前文本相似度处理之后的相似视频作为比较集
            else if (Const.STORM_CONFIG.IS_FILTER_AND_REFINE && ctrlMsg.containsKey("textSimilarVideoList")) {

                // 解析文本内容相似的视频列表
                List<TextSimilarVideo> textSimilarVideoList     = new ArrayList<TextSimilarVideo>();
                String                 textSimilarVideoListStr  = ctrlMsg.get("textSimilarVideoList");
                Type                   textSimilarVideoListType = new TypeToken<List<TextSimilarVideo>>() {}
                .getType();

                textSimilarVideoList = (new Gson()).fromJson(textSimilarVideoListStr, textSimilarVideoListType);

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
            Type queryLocalSigType = new TypeToken<List<List<KDFeaturePoint>>>() {}
            .getType();

            // query视频的标签,list大小为query视频帧图像的数量,内层list大小为SIFT特征点的个数
            List<List<KDFeaturePoint>> queryLocalSigs = (new Gson()).fromJson(queryLocalSigStr, queryLocalSigType);

            // 保存局部特征相似的视频
            List<LocalSimilarVideo> localSimilarVideoList = new ArrayList<LocalSimilarVideo>();

            // 如果没有局部标签,则输出空列表
            if ((null == queryLocalSigs) || queryLocalSigs.isEmpty()) {
                String localSimilarVideoListStr = (new Gson()).toJson(localSimilarVideoList);

                ctrlMsg.put("localSimilarVideoList", localSimilarVideoListStr);

                // 移除不必要的key
                if (Const.SSM_CONFIG.IS_REDUCTIION) {
                    ctrlMsg = Const.SSM_CONFIG.discardInvalidKey("LocalSigDistanceBolt", ctrlMsg);
                }

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

                // 指定时长的视频的SIFT标签文件
                String comparedVideoSIFTFilePath = Const.CC_WEB_VIDEO.SIFT_SIGNATURE_PATH_PREFIX
                                                   + Integer.parseInt(comparedVideoId) / 100 + "/" + comparedVideoId
                                                   + ".txt";
                File comparedVideoSIFTFile = new File(comparedVideoSIFTFilePath);

                if (!comparedVideoSIFTFile.exists()) {
                    continue;
                }

                try {
                    BufferedReader reader = new BufferedReader(new FileReader(comparedVideoSIFTFile));
                    String         line   = reader.readLine();

                    // 文件的一行代表一个compare视频
                    if (null != line) {

                        // compare视频的SIFT标签
                        VideoSIFTSigEntity comparedVideoLocalSig = (new Gson()).fromJson(line,
                                                                                         VideoSIFTSigEntity.class);

                        // 如果比较视频的局部标签不存在,则继续处理下个视频
                        if ((null == comparedVideoLocalSig) || comparedVideoLocalSig.getSignature().isEmpty()) {
                            continue;
                        }

                        // compare视频各个帧图像的SIFT标签
                        List<SIFTSigEntity> comparedKeyframeSigs = comparedVideoLocalSig.getSignature();

                        // 记录相似的帧图像数量
                        int similarKeyframeNum = 0;

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
                                    List<Match> ms = MatchKeys.findMatchesBBF(queryLocalSigs.get(i),
                                                                              comparedKeyframeSigs.get(j).getSig());

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

                        float localSigSimilarity = (float) similarKeyframeNum / (float) queryLocalSigs.size();

                        if (localSigSimilarity >= Const.STORM_CONFIG.LOCALSIG_VIDEO_SIMILARITY_THRESHOLd) {
                            LocalSimilarVideo localSimilarVideo = new LocalSimilarVideo(comparedVideoId,
                                                                                        localSigSimilarity);

                            localSimilarVideoList.add(localSimilarVideo);
                        }
                    }

                    reader.close();
                } catch (FileNotFoundException e) {
                    logger.error("file not found: " + comparedVideoSIFTFilePath, e);
                } catch (IOException e) {
                    logger.error("io error when read file: " + comparedVideoSIFTFilePath, e);
                }
            }

            Collections.sort(localSimilarVideoList, new LocalSimilarVideo());
            logger.info("Local similar video size: " + localSimilarVideoList.size() + ", taskId: " + taskId);

            // 在控制信息中加入局部特征相似视频列表,有可能为空!
            String localSimilarVideoListStr = (new Gson()).toJson(localSimilarVideoList);

            ctrlMsg.put("localSimilarVideoList", localSimilarVideoListStr);

            // logger.info("put localSimilarVideoList: " + localSimilarVideoListStr);
            // 移除不必要的key
            if (Const.SSM_CONFIG.IS_REDUCTIION) {
                ctrlMsg = Const.SSM_CONFIG.discardInvalidKey("LocalSigDistanceBolt", ctrlMsg);
            }

            // bolt输出
            collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));

            int msgLength = 0;

            for (Map.Entry<String, String> entry : ctrlMsg.entrySet()) {
                msgLength += entry.getKey().length() + entry.getValue().length();
            }

            logger.info("Control message size in local feature distance: " + msgLength + ", taskId: " + taskId);
        } else if (Const.STORM_CONFIG.DETECTION_TASK_FLAG.equals(taskType)) {

            // 第一个视频的信息
            String          queryVideoInfoStr = ctrlMsg.get("queryVideo");
            VideoInfoEntity queryVideoInfo    = (new Gson()).fromJson(queryVideoInfoStr, VideoInfoEntity.class);

            // 获得第一个视频的局部标签
            String queryLocalSigStr = ctrlMsg.get("localSignature");

            // 视频SIFT标签的type
            Type queryLocalSigType = new TypeToken<List<List<KDFeaturePoint>>>() {}
            .getType();

            // 第一个视频的标签,list大小为query视频帧图像的数量,内层list大小为SIFT特征点的个数
            List<List<KDFeaturePoint>> queryLocalSigs = (new Gson()).fromJson(queryLocalSigStr, queryLocalSigType);

            // 第二个视频的信息
            String          queryVideoInfoStr2 = ctrlMsg.get("queryVideo2");
            VideoInfoEntity queryVideoInfo2    = (new Gson()).fromJson(queryVideoInfoStr2, VideoInfoEntity.class);

            // 获得第二个视频的局部标签
            String queryLocalSigStr2 = ctrlMsg.get("localSignature2");

            // 第二个视频的标签,list大小为视频帧图像的数量,内层list大小为SIFT特征点的个数
            List<List<KDFeaturePoint>> queryLocalSigs2 = (new Gson()).fromJson(queryLocalSigStr2, queryLocalSigType);

            // 如果没有局部标签,则相似度设为0并输出
            if ((null == queryLocalSigs)
                    || queryLocalSigs.isEmpty()
                    || (null == queryLocalSigs2)
                    || queryLocalSigs2.isEmpty()) {
                ctrlMsg.put("localSimilarity", Float.toString((float) 0.0));

                // bolt输出
                collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));

                return;
            }

            // 记录相似的帧图像数量
            int similarKeyframeNum = 0;

            // i表示第一个视频的帧序号,依次将第一个视频的每个帧与第二个视频进行比较
            for (int i = 0; i < queryLocalSigs.size(); i++) {

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
                    if (queryLocalSigs.get(i).isEmpty() || queryLocalSigs2.get(j).isEmpty()) {
                        continue;
                    }

                    try {

                        // 比较query视频和compare视频
                        List<Match> ms = MatchKeys.findMatchesBBF(queryLocalSigs.get(i), queryLocalSigs2.get(j));

                        ms = MatchKeys.filterMore(ms);

                        // 如果找到一个相似度大于阈值的帧,则开始比较query视频的下一个帧图像
                        if ((float) ms.size() / (float) queryLocalSigs.size()
                                >= Const.STORM_CONFIG.LOCALSIG_KEYFRAME_SIMILARITY_THRESHOLD) {
                            similarKeyframeNum++;

                            break;
                        }
                    } catch (IllegalArgumentException e) {
                        logger.error("QueryVideoId: " + queryVideoInfo.getVideoId() + " | "
                                     + queryVideoInfo2.getVideoId() + ". " + e);
                    }
                }
            }

            float localSigSimilarity = (float) similarKeyframeNum / (float) queryLocalSigs.size();

            // 在控制信息中加入局部特征的相似度
            ctrlMsg.put("localSimilarity", Float.toString(localSigSimilarity));

            // bolt输出
            collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
        }
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {}

    /**
     * Prepare.
     *
     * @param stormConf the storm conf
     * @param context   the context
     * @see backtype.storm.topology.base.BaseBasicBolt#prepare(java.util.Map,
     * backtype.storm.task.TopologyContext) backtype.storm.topology.base.BaseBasicBolt#prepare(java.util.Map,
     * backtype.storm.task.TopologyContext)
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);

        VideoInfoDao          videoInfoDao      = new VideoInfoDao();
        List<VideoInfoEntity> zeroVideoInfoList = videoInfoDao.getVideoInfoByDuration(0);    // 取出时长为0的视频(数据集中有些视频没有duration数据,我们设为0)

        if ((null != zeroVideoInfoList) &&!zeroVideoInfoList.isEmpty()) {
            Set<String> zeroVideoIdList = new HashSet<String>();

            for (VideoInfoEntity videoInfo : zeroVideoInfoList) {
                zeroVideoIdList.add(videoInfo.getVideoId());
            }

            this.cachedVideoIdByDuration.put(0, zeroVideoIdList);    // 将时长为0的视频id集合缓存到按时长存储的Map中
        }
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
