/**
 * @Package cn.pku.net.db.storm.ndvr.customized
 * Created by jeremyjiang on 2016/5/12.
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
import cn.pku.net.db.storm.ndvr.dao.HSVSignatureDao;
import cn.pku.net.db.storm.ndvr.dao.KeyFrameDao;
import cn.pku.net.db.storm.ndvr.dao.VideoInfoDao;
import cn.pku.net.db.storm.ndvr.entity.*;
import cn.pku.net.db.storm.ndvr.util.GlobalSigGenerator;
import cn.pku.net.db.storm.ndvr.util.MyStringUtils;
import cn.pku.net.db.storm.ndvr.util.SigSim;
import com.google.gson.Gson;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Description: Customized bolt for pre-filtering retrieval task, generate and compare textual and global visual signature
 * @author jeremyjiang
 * Created at 2016/5/12 20:46
 */
public class CusTextGlobalPreRetriBolt extends BaseBasicBolt {
    private static final Logger              logger                  =
        Logger.getLogger(CusTextGlobalPreRetriBolt.class);
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
        declarer.declare(new Fields("taskId", "taskType", "queryVideo", "similarVideoList",
                                    "startTimeStamp", "fieldGroupingId"));
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

            // 如果cache中没有对应时长的视频,则查询数据库
            if (!this.cachedVideoIdByDuration.containsKey(duration)) {
                List<VideoInfoEntity> videoInfosByDuration = (new VideoInfoDao()).getVideoInfoByDuration(duration);
                Set<String>           videoIdSet           = new HashSet<String>();

                for (VideoInfoEntity videoInfoEnt : videoInfosByDuration) {
                    videoIdSet.add(videoInfoEnt.getVideoId());
                }

                if (videoIdSet.isEmpty()) {

                    // 存入cache
                    this.cachedVideoIdByDuration.put(duration, videoIdSet);
                    logger.info("Cache duration:" + duration + ", size:" + videoIdSet.size());
                }

                // 存入待比较视频列表
                comparedVideoIdSet.addAll(videoIdSet);
            }

            // 如果cache中有对应时长的视频,则直接查询内存的Map
            else {
                comparedVideoIdSet.addAll(this.cachedVideoIdByDuration.get(duration));
            }
        }

        // 保存text相似的视频
        List<TextSimilarVideo> textSimilarVideoList = new ArrayList<TextSimilarVideo>();

        // query视频的文本信息
        String queryVideoText = queryVideo.getTitle();

        if (null == queryVideoText) {
            collector.emit(new Values(taskId, taskType, queryVideoStr, null, startTimeStamp, fieldGroupingId));

            return;
        }

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
            float textSimilarity = queryVScompared * comparedVSquery / (queryVScompared + comparedVSquery);

            // 如果相似度大于阈值,存入相似列表
            if (textSimilarity >= Const.STORM_CONFIG.TEXT_SIMILARITY_THRESHOLD) {
                TextSimilarVideo textSimilarVideo = new TextSimilarVideo(comparedVideoId, textSimilarity);

                textSimilarVideoList.add(textSimilarVideo);
            }
        }

        // if there is no text similar video, return null
        if (textSimilarVideoList.isEmpty()) {
            collector.emit(new Values(taskId, taskType, queryVideoStr, null, startTimeStamp, fieldGroupingId));

            return;
        }

        // global feature
        List<KeyFrameEntity> keyframeList = (new KeyFrameDao()).getKeyFrameByVideoId(queryVideo.getVideoId());

        // 如果该视频没有对应的关键帧信息,则将全局标签设为null并输出
        if ((null == keyframeList) || keyframeList.isEmpty()) {
            collector.emit(new Values(taskId, taskType, queryVideoStr, null, startTimeStamp, fieldGroupingId));

            return;
        }

        Collections.sort(keyframeList, new KeyFrameEntity());

        String            keyframeListStr   = (new Gson()).toJson(keyframeList);
        HSVSigEntity      queryHsvSignature = GlobalSigGenerator.generate(keyframeList);
        VideoHSVSigEntity queryVideoHsvSig  = new VideoHSVSigEntity(queryVideo.getVideoId(), queryHsvSignature);

        // 输出结果,保存相似的视频
        List<GlobalSimilarVideo> globalSimilarVideoList = new ArrayList<GlobalSimilarVideo>();

        // 如果query全局标签为空,则输出空列表
        if ((null == queryVideoHsvSig) || (null == queryVideoHsvSig.getSig())) {
            String globalSimVideoListStr = (new Gson()).toJson(globalSimilarVideoList);

            // bolt输出
            collector.emit(new Values(taskId, taskType, queryVideoStr, null, startTimeStamp, fieldGroupingId));

            return;
        }

        // 待比较视频的id集合(唯一集合),pre-filtering,textual similar的视频集合
        comparedVideoIdSet.clear();

        for (TextSimilarVideo similarVideo : textSimilarVideoList) {
            comparedVideoIdSet.add(similarVideo.getVideoId());
        }

        // 依次比较compare视频和query视频
        for (String comparedVideoId : comparedVideoIdSet) {

            // 如果为检索视频本身，则跳过
            if (comparedVideoId.equals(queryVideo.getVideoId())) {
                continue;
            }

            VideoHSVSigEntity comparedVideoHsvSig = null;

            // 如果缓存中没有compare视频的HSV标签,则将新查询到的视频标签存入缓存
            if (!cachedHSVSignature.containsKey(comparedVideoId)) {
                comparedVideoHsvSig = (new HSVSignatureDao()).getVideoHSVSigById(comparedVideoId);

                // 如果数据库中没有视频对应的全局标签,则处理下个待比较的视频
                if (null == comparedVideoHsvSig) {
                    logger.info("During comparing, no signature found in database, videoId: " + comparedVideoId);

                    continue;
                }

                this.cachedHSVSignature.put(comparedVideoId, comparedVideoHsvSig.getSig());

                // logger.info("cache hsv signature, videoId: "
                // + comparedVideoId + ", duration: "
                // + duration);
            }

            // 如果缓存中有compare视频的HSV标签,则查询缓存
            else {
                comparedVideoHsvSig = new VideoHSVSigEntity(comparedVideoId, cachedHSVSignature.get(comparedVideoId));
            }

            float euclideanDistance = SigSim.getEuclideanDistance(queryVideoHsvSig.getSig(),
                                                                  comparedVideoHsvSig.getSig());

            // logger.info("EuclideanDistance: " + euclideanDistance + ", queryVideoId: "
            // + queryVideo.getVideoId() + ", comparedVideoId: " + comparedVideoId);
            if (euclideanDistance <= Const.STORM_CONFIG.GLOBALSIG_EUCLIDEAN_THRESHOLD) {
                globalSimilarVideoList.add(new GlobalSimilarVideo(comparedVideoId, euclideanDistance));
            }
        }

        // for (GlobalSimilarVideo similarVideo : globalSimilarVideoList) {
        // logger.info("Global Similar video, videoId: " + similarVideo.getVideoId() + ", distance: "
        // + similarVideo.getGlobalSigEucliDistance());
        // }
        // 按照距离从小到大进行排序
        Collections.sort(globalSimilarVideoList, new GlobalSimilarVideo());

        // 在控制信息中加入相似视频列表,有可能为空!
        logger.info("Global similar video size: " + globalSimilarVideoList.size());

        String globalSimVideoListStr = (new Gson()).toJson(globalSimilarVideoList);

        collector.emit(new Values(taskId,
                                  taskType,
                                  queryVideoStr,
                                  globalSimVideoListStr,
                                  startTimeStamp,
                                  fieldGroupingId));
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
