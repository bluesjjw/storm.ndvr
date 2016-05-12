/**
 * @Title: CustomizedGlobalRetrievalBolt2.java 
 * @Package cn.pku.net.db.storm.ndvr.customized 
 * @Description: TODO
 * @author Jiawei Jiang    
 * @date 2015年2月9日 下午7:08:29 
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved.
 */
package cn.pku.net.db.storm.ndvr.customized;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.pku.net.db.storm.ndvr.bolt.GlobalSigDistanceRetrievalBolt;
import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.dao.HSVSignatureDao;
import cn.pku.net.db.storm.ndvr.dao.VideoInfoDao;
import cn.pku.net.db.storm.ndvr.entity.GlobalSimilarVideo;
import cn.pku.net.db.storm.ndvr.entity.HSVSigEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoHSVSigEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoInfoEntity;

import com.google.gson.Gson;

/**
 * @ClassName: CustomizedGlobalRetrievalBolt2 
 * @Description: TODO
 * @author Jiawei Jiang
 * @date 2015年2月9日 下午7:08:29
 */
public class CustomizedGlobalRetrievalBolt2 extends BaseBasicBolt {

    private static final Logger              logger                  = Logger
                                                                         .getLogger(CustomizedGlobalRetrievalBolt2.class);
    private static Map<Integer, Set<String>> cachedVideoIdByDuration = new ConcurrentHashMap<Integer, Set<String>>();     //缓存视频数据,key为duration,value为视频元数据
    private static Map<String, HSVSigEntity> cachedHSVSignature      = new ConcurrentHashMap<String, HSVSigEntity>();     //缓存视频的HSV全局标签,key为视频id,value为视频HSV全局标签

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

        //获得检索视频的全局标签
        String queryGlobalSigStr = input.getStringByField("globalSignature");
        VideoHSVSigEntity queryVideoHsvSig = (new Gson()).fromJson(queryGlobalSigStr,
            VideoHSVSigEntity.class);

        //输出结果,保存相似的视频
        List<GlobalSimilarVideo> globalSimilarVideoList = new ArrayList<GlobalSimilarVideo>();

        //如果query全局标签为空,则输出空列表
        if (null == queryVideoHsvSig || null == queryVideoHsvSig.getSig()) {
            //bolt输出
            collector.emit(new Values(taskId, taskType, globalSimilarVideoList, startTimeStamp,
                fieldGroupingId));
            return;
        }

        //待比较视频的id集合(唯一集合),根据视频id即可以在数据库找到该视频的全局标签
        Set<String> comparedVideoIdSet = new HashSet<String>();
        //待检索视频的时长
        int queryVideoDuration = queryVideo.getDuration();
        //计算视频时长比较窗口的大小
        int videoDurationWindowMin = queryVideoDuration - Const.STORM_CONFIG.VIDEO_DURATION_WINDOW;
        if (videoDurationWindowMin <= 0) {
            videoDurationWindowMin = 1;
        }
        int videoDurationWindowMax = queryVideoDuration + Const.STORM_CONFIG.VIDEO_DURATION_WINDOW;
        for (int duration = videoDurationWindowMin; duration <= videoDurationWindowMax; duration++) {
            //如果cache中没有对应时长的视频,则查询数据库
            if (!this.cachedVideoIdByDuration.containsKey(duration)) {
                List<VideoInfoEntity> videoInfosByDuration = (new VideoInfoDao())
                    .getVideoInfoByDuration(duration);
                Set<String> videoIdSet = new HashSet<String>();
                for (VideoInfoEntity videoInfoEnt : videoInfosByDuration) {
                    videoIdSet.add(videoInfoEnt.getVideoId());
                }
                if (videoIdSet.isEmpty()) {
                    //存入cache
                    this.cachedVideoIdByDuration.put(duration, videoIdSet);
                    logger.info("Cache duration:" + duration + ", size:" + videoIdSet.size());
                }
                //存入待比较视频列表
                comparedVideoIdSet.addAll(videoIdSet);
            }
            //如果cache中有对应时长的视频,则直接查询内存的Map
            else {
                comparedVideoIdSet.addAll(this.cachedVideoIdByDuration.get(duration));
            }
        }

        //        logger.info("Compared video size: " + comparedVideoIdSet.size());

        //依次比较compare视频和query视频
        for (String comparedVideoId : comparedVideoIdSet) {
            //如果为检索视频本身，则跳过
            if (comparedVideoId.equals(queryVideo.getVideoId())) {
                continue;
            }
            VideoHSVSigEntity comparedVideoHsvSig = null;

            //如果缓存中没有compare视频的HSV标签,则将新查询到的视频标签存入缓存
            if (!cachedHSVSignature.containsKey(comparedVideoId)) {
                comparedVideoHsvSig = (new HSVSignatureDao()).getVideoHSVSigById(comparedVideoId);
                //如果数据库中没有视频对应的全局标签,则处理下个待比较的视频
                if (null == comparedVideoHsvSig) {
                    logger.info("During comparing, no signature found in database, videoId: "
                                + comparedVideoId);
                    continue;
                }
                this.cachedHSVSignature.put(comparedVideoId, comparedVideoHsvSig.getSig());
                //                                logger.info("cache hsv signature, videoId: "
                //                                            + comparedVideoId + ", duration: "
                //                                            + duration);
            }
            //如果缓存中有compare视频的HSV标签,则查询缓存
            else {
                comparedVideoHsvSig = new VideoHSVSigEntity(comparedVideoId,
                    cachedHSVSignature.get(comparedVideoId));
            }
            float euclideanDistance = GlobalSigDistanceRetrievalBolt.getGlobalDistance(
                queryVideoHsvSig.getSig(), comparedVideoHsvSig.getSig());
            //            logger.info("EuclideanDistance: " + euclideanDistance + ", queryVideoId: "
            //                        + queryVideo.getVideoId() + ", comparedVideoId: " + comparedVideoId);
            if (euclideanDistance <= Const.STORM_CONFIG.GLOBALSIG_EUCLIDEAN_THRESHOLD) {
                globalSimilarVideoList.add(new GlobalSimilarVideo(comparedVideoId,
                    euclideanDistance));
            }
        }

        //            for (GlobalSimilarVideo similarVideo : globalSimilarVideoList) {
        //                logger.info("Global Similar video, videoId: " + similarVideo.getVideoId() + ", distance: "
        //                            + similarVideo.getGlobalSigEucliDistance());
        //            }
        //按照距离从小到大进行排序
        Collections.sort(globalSimilarVideoList, new GlobalSimilarVideo());
        //在控制信息中加入相似视频列表,有可能为空!
        //        logger.info("Global similar video size: " + globalSimilarVideoList.size());
        String globalSimVideoListStr = (new Gson()).toJson(globalSimilarVideoList);
        collector.emit(new Values(taskId, taskType, globalSimVideoListStr, startTimeStamp,
            fieldGroupingId));
    }

    /** 
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("taskId", "taskType", "globalSimilarVideoList",
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
