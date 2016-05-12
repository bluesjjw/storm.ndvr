/**
 * @Title: CustomizedGlobalLocalRetrievalBolt.java 
 * @Package cn.pku.net.db.storm.ndvr.customized.text 
 * @Description: TODO
 * @author Jiawei Jiang    
 * @date 2015年2月2日 下午4:17:41 
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved.
 */
package cn.pku.net.db.storm.ndvr.customized;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.imageio.ImageIO;

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
import cn.pku.net.db.storm.ndvr.dao.KeyFrameDao;
import cn.pku.net.db.storm.ndvr.dao.VideoInfoDao;
import cn.pku.net.db.storm.ndvr.entity.GlobalSimilarVideo;
import cn.pku.net.db.storm.ndvr.entity.HSVSigEntity;
import cn.pku.net.db.storm.ndvr.entity.KeyFrameEntity;
import cn.pku.net.db.storm.ndvr.entity.LocalSimilarVideo;
import cn.pku.net.db.storm.ndvr.entity.SIFTSigEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoHSVSigEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoInfoEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoSIFTSigEntity;
import cn.pku.net.db.storm.ndvr.image.analyze.sift.SIFT;
import cn.pku.net.db.storm.ndvr.image.analyze.sift.match.Match;
import cn.pku.net.db.storm.ndvr.image.analyze.sift.match.MatchKeys;
import cn.pku.net.db.storm.ndvr.image.analyze.sift.render.RenderImage;
import cn.pku.net.db.storm.ndvr.image.analyze.sift.scale.KDFeaturePoint;
import cn.pku.net.db.storm.ndvr.util.GlobalSigGenerator;

import com.google.gson.Gson;

/**
 * @ClassName: CustomizedGlobalLocalRetrievalBolt 
 * @Description: TODO
 * @author Jiawei Jiang
 * @date 2015年2月2日 下午4:17:41
 */
public class CustomizedGlobalLocalRetrievalBolt extends BaseBasicBolt {

    private static final Logger              logger                  = Logger
                                                                         .getLogger(CustomizedGlobalLocalRetrievalBolt.class);
    private static Map<Integer, Set<String>> cachedVideoIdByDuration = new ConcurrentHashMap<Integer, Set<String>>();         //缓存视频数据,key为duration,value为视频元数据
    private static Map<String, HSVSigEntity> cachedHSVSignature      = new ConcurrentHashMap<String, HSVSigEntity>();         //缓存视频的HSV全局标签,key为视频id,value为视频HSV全局标签

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
        List<KeyFrameEntity> keyframeList = (new KeyFrameDao()).getKeyFrameByVideoId(queryVideo
            .getVideoId());
        //如果该视频没有对应的关键帧信息,则将全局标签设为null并输出
        if (null == keyframeList || keyframeList.isEmpty()) {
            collector.emit(new Values(taskId, taskType, queryVideoStr, null, startTimeStamp,
                fieldGroupingId));
            return;
        }
        Collections.sort(keyframeList, new KeyFrameEntity());
        HSVSigEntity queryHsvSignature = GlobalSigGenerator.generate(keyframeList);
        VideoHSVSigEntity queryVideoHsvSig = new VideoHSVSigEntity(queryVideo.getVideoId(),
            queryHsvSignature);

        //输出结果,保存相似的视频
        List<GlobalSimilarVideo> globalSimilarVideoList = new ArrayList<GlobalSimilarVideo>();

        //如果query全局标签为空,则输出空列表
        if (null == queryVideoHsvSig || null == queryVideoHsvSig.getSig()) {
            //bolt输出
            collector.emit(new Values(taskId, taskType, queryVideoStr, null, startTimeStamp,
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
        logger.info("Global similar video size: " + globalSimilarVideoList.size());

        if (null == globalSimilarVideoList || globalSimilarVideoList.isEmpty()) {
            collector.emit(new Values(taskId, taskType, queryVideoStr, null, startTimeStamp,
                fieldGroupingId));
            return;
        }

        //待比较视频的id集合(唯一集合),根据视频id即可以找到该视频的SIFT标签文件
        comparedVideoIdSet.clear();
        for (GlobalSimilarVideo similarVideo : globalSimilarVideoList) {
            comparedVideoIdSet.add(similarVideo.getVideoId());
        }

        //保存视频的SIFT标签,list每个元素为一个帧的SIFT关键点
        List<List<KDFeaturePoint>> queryLocalSigs = new ArrayList<List<KDFeaturePoint>>();
        for (int i = 0; i < keyframeList.size(); i++) {
            KeyFrameEntity keyframeEnt = keyframeList.get(i);
            String keyframeFile = Const.CC_WEB_VIDEO.KEYFRAME_FILE_PATH_PREFIX
                                  + Integer.parseInt(keyframeEnt.getVideoId()) / 100 + "/"
                                  + keyframeEnt.getKeyFrameName();
            try {
                BufferedImage img = ImageIO.read(new File(keyframeFile));
                RenderImage ri = new RenderImage(img);
                SIFT sift = new SIFT();
                sift.detectFeatures(ri.toPixelFloatArray(null));
                List<KDFeaturePoint> al = sift.getGlobalKDFeaturePoints();
                queryLocalSigs.add(al);
            } catch (IOException e1) {
                logger.error("IO error when read image: " + keyframeFile, e1);
            } catch (java.lang.ArrayIndexOutOfBoundsException e2) {
                logger.error("Array index out of bounds: " + keyframeFile, e2);
            }
        }

        //如果没有局部标签,则输出空列表
        if (null == queryLocalSigs || queryLocalSigs.isEmpty()) {
            collector.emit(new Values(taskId, taskType, queryVideoStr, null, startTimeStamp,
                fieldGroupingId));
            return;
        }

        //保存局部特征相似的视频
        List<LocalSimilarVideo> localSimilarVideoList = new ArrayList<LocalSimilarVideo>();
        //依次和视频时长在窗口内的视频比较
        for (String comparedVideoId : comparedVideoIdSet) {
            //如果为检索视频本身,则跳过
            if (comparedVideoId.equals(queryVideo.getVideoId())) {
                continue;
            }

            //指定时长的视频的SIFT标签文件
            String comparedVideoSIFTFilePath = Const.CC_WEB_VIDEO.SIFT_SIGNATURE_PATH_PREFIX
                                               + Integer.parseInt(comparedVideoId) / 100 + "/"
                                               + comparedVideoId + ".txt";
            File comparedVideoSIFTFile = new File(comparedVideoSIFTFilePath);
            if (!comparedVideoSIFTFile.exists()) {
                continue;
            }
            try {
                BufferedReader reader = new BufferedReader(new FileReader(comparedVideoSIFTFile));
                String line = reader.readLine();
                //文件的一行代表一个compare视频
                if (null != line) {
                    //compare视频的SIFT标签
                    VideoSIFTSigEntity comparedVideoLocalSig = (new Gson()).fromJson(line,
                        VideoSIFTSigEntity.class);
                    //如果比较视频的局部标签不存在,则继续处理下个视频
                    if (null == comparedVideoLocalSig
                        || comparedVideoLocalSig.getSignature().isEmpty()) {
                        continue;
                    }
                    //compare视频各个帧图像的SIFT标签
                    List<SIFTSigEntity> comparedKeyframeSigs = comparedVideoLocalSig.getSignature();
                    //记录相似的帧图像数量
                    int similarKeyframeNum = 0;
                    //i表示query视频的帧序号,依次将query视频的每个帧与compare视频进行比较
                    for (int i = 0; i < queryLocalSigs.size(); i++) {
                        //compare视频帧图像的比较窗口边界
                        int comparedFrameLeft = i - Const.STORM_CONFIG.FRAME_COMPARED_WINDOW;
                        if (comparedFrameLeft < 0) {
                            comparedFrameLeft = 0;
                        }
                        int comparedFrameRight = i + Const.STORM_CONFIG.FRAME_COMPARED_WINDOW;
                        if (comparedFrameRight >= comparedKeyframeSigs.size()) {
                            comparedFrameRight = comparedKeyframeSigs.size() - 1;
                        }

                        //j表示compare视频的帧序号
                        for (int j = comparedFrameLeft; j <= comparedFrameRight; j++) {
                            if (queryLocalSigs.get(i).isEmpty()
                                || comparedKeyframeSigs.get(j).getSig().isEmpty()) {
                                continue;
                            }
                            try {
                                //比较query视频和compare视频
                                List<Match> ms = MatchKeys.findMatchesBBF(queryLocalSigs.get(i),
                                    comparedKeyframeSigs.get(j).getSig());
                                ms = MatchKeys.filterMore(ms);
                                //如果找到一个相似度大于阈值的帧,则开始比较query视频的下一个帧图像
                                if (ms.size() / (float) queryLocalSigs.get(i).size() >= Const.STORM_CONFIG.LOCALSIG_KEYFRAME_SIMILARITY_THRESHOLD) {
                                    similarKeyframeNum++;
                                    break;
                                }
                            } catch (IllegalArgumentException e) {
                                logger.error("ComparedVideoId: " + comparedVideoId + ", " + e);
                            }
                        }
                    }
                    float localSigSimilarity = similarKeyframeNum / (float) queryLocalSigs.size();
                    if (localSigSimilarity >= Const.STORM_CONFIG.LOCALSIG_VIDEO_SIMILARITY_THRESHOLd) {
                        LocalSimilarVideo localSimilarVideo = new LocalSimilarVideo(
                            comparedVideoId, localSigSimilarity);
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
        logger.info("Local similar video size: " + localSimilarVideoList.size());
        String localSimilarVideoListStr = (new Gson()).toJson(localSimilarVideoList);
        collector.emit(new Values(taskId, taskType, queryVideoStr, localSimilarVideoListStr,
            startTimeStamp, fieldGroupingId));
    }

    /** 
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("taskId", "taskType", "queryVideo", "similarVideoList",
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
