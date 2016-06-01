/**
 * @Package cn.pku.net.db.storm.ndvr
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */
package cn.pku.net.db.storm.ndvr;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.imageio.ImageIO;

import cn.pku.net.db.storm.ndvr.general.TextSimilarBolt;
import cn.pku.net.db.storm.ndvr.util.MyStringUtils;
import cn.pku.net.db.storm.ndvr.util.SigSim;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.wltea.analyzer.lucene.IKAnalyzer;

import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.dao.HSVSignatureDao;
import cn.pku.net.db.storm.ndvr.dao.KeyFrameDao;
import cn.pku.net.db.storm.ndvr.dao.TaskDao;
import cn.pku.net.db.storm.ndvr.dao.TaskResultDao;
import cn.pku.net.db.storm.ndvr.dao.VideoInfoDao;
import cn.pku.net.db.storm.ndvr.entity.GlobalSimilarVideo;
import cn.pku.net.db.storm.ndvr.entity.HSVSigEntity;
import cn.pku.net.db.storm.ndvr.entity.KeyFrameEntity;
import cn.pku.net.db.storm.ndvr.entity.LocalSimilarVideo;
import cn.pku.net.db.storm.ndvr.entity.SIFTSigEntity;
import cn.pku.net.db.storm.ndvr.entity.TaskEntity;
import cn.pku.net.db.storm.ndvr.entity.TextSimilarVideo;
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
import com.google.gson.reflect.TypeToken;

/**
 * Description: Test app
 *
 * @author jeremyjiang
 * Created at 2016/5/12 18:28
 */
public class Test {

    private static final Logger logger = Logger.getLogger(Test.class);

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {
        boolean debug = false;

        Map<Integer, Set<String>> cachedVideoIdByDuration = new ConcurrentHashMap<Integer, Set<String>>(); // 缓存视频数据,key为duration,value为视频元数据
        Map<String, String> cachedVideoText = new ConcurrentHashMap<String, String>(); // 缓存视频数据,key为视频id,value为视频文本信息
        Map<String, HSVSigEntity> cachedHSVSignature = new ConcurrentHashMap<String, HSVSigEntity>(); // 缓存视频的HSV全局标签,key为视频id,value为视频HSV全局标签
        VideoInfoDao videoInfoDao = new VideoInfoDao();
        List<VideoInfoEntity> zerovideoInfoList = videoInfoDao
                .getVideoInfoByDuration(0); // 取出时长为0的视频(数据集中有些视频没有duration数据,我们设为0)
        if (null != zerovideoInfoList && !zerovideoInfoList.isEmpty()) {
            Set<String> videoIdSet = new HashSet<String>();
            for (VideoInfoEntity videoInfoEnt : zerovideoInfoList) {
                videoIdSet.add(videoInfoEnt.getVideoId());
                if (null != videoInfoEnt.getTitle()) {
                    videoIdSet.add(videoInfoEnt.getVideoId());
                    cachedVideoText.put(videoInfoEnt.getVideoId(),
                            videoInfoEnt.getTitle()); // 缓存时长为0的视频的HSV全局标签
                }
                VideoHSVSigEntity videoHsvSig = (new HSVSignatureDao())
                        .getVideoHSVSigById(videoInfoEnt.getVideoId());
                // 如果数据库中没有该视频的HSV标签,则继续下一个视频
                if (null == videoHsvSig) {
                    continue;
                }
                cachedHSVSignature.put(videoHsvSig.getVideoId(), videoHsvSig.getSig()); // 缓存时长为0的视频的HSV全局标签
            }
            cachedVideoIdByDuration.put(0, videoIdSet); // 将时长为0的视频缓存
        }
        logger.info("cachedVideoInfos size: " + cachedVideoIdByDuration.size());
        logger.info("cachedHSVSignature size: " + cachedHSVSignature.size());

        Map<String, String> ctrlMsg = new HashMap<String, String>(); // 控制信息,key为String,value为Gson生成的JSON字符串
        TaskDao taskDao = new TaskDao();
        List<TaskEntity> taskList = taskDao.getNewTask();
        logger.info("Task size: " + taskList.size());
        for (TaskEntity task : taskList) {
            long taskStartTime = System.currentTimeMillis();
            String taskId = task.getTaskId();
            String taskType = task.getTaskType();
            List<VideoInfoEntity> videoInfoList = new ArrayList<VideoInfoEntity>();

            // 取出task对应的query视频
            // 取出task对应的query视频id
            List<String> videoIdList = task.getVideoIdList();
            for (String videoId : videoIdList) {
                videoInfoList.add(videoInfoDao.getVideoInfoById(videoId));
            }
            if (Const.STORM_CONFIG.RETRIEVAL_TASK_FLAG.equals(taskType)) {
                VideoInfoEntity ent = videoInfoList.get(0);
                Gson gson = new Gson();
                String gsonStr = gson.toJson(ent);
                ctrlMsg.put("queryVideo", gsonStr);
                logger.info("put queryVideo: " + gsonStr);
            } else if (Const.STORM_CONFIG.DETECTION_TASK_FLAG.equals(taskType)) {
                for (VideoInfoEntity ent1 : videoInfoList) {
                    for (VideoInfoEntity ent2 : videoInfoList) {
                        // 保证第一个视频的id小于第二个视频的id
                        if (Integer.parseInt(ent1.getVideoId()) < Integer.parseInt(ent2
                                .getVideoId())) {
                            Gson gson = new Gson();
                            String gsonStr = gson.toJson(ent1);
                            ctrlMsg.put("queryVideo", gsonStr);
                            logger.info("put queryVideo: " + gsonStr);
                            gsonStr = gson.toJson(ent2);
                            ctrlMsg.put("queryVideo2", gsonStr);
                            logger.info("put queryVideo2: " + gsonStr);
                        }
                    }
                }
            }

            //text similarity
            if (Const.STORM_CONFIG.RETRIEVAL_TASK_FLAG.equals(taskType)) {
                String queryVideoInfoStr = ctrlMsg.get("queryVideo");
                VideoInfoEntity queryVideoInfo = (new Gson()).fromJson(queryVideoInfoStr,
                        VideoInfoEntity.class);

                // 待比较视频的id集合(唯一集合),根据视频id即可以在数据库找到该视频的全局标签
                Set<String> comparedVideoIdSet = new HashSet<String>();

                // 待检索视频的时长
                int queryVideoDuration = queryVideoInfo.getDuration();
                // 计算视频时长比较窗口的大小
                int videoDurationWindowMin = queryVideoDuration
                        - Const.STORM_CONFIG.VIDEO_DURATION_WINDOW;
                if (videoDurationWindowMin <= 0) {
                    videoDurationWindowMin = 1;
                }
                int videoDurationWindowMax = queryVideoDuration
                        + Const.STORM_CONFIG.VIDEO_DURATION_WINDOW;

                for (int duration = videoDurationWindowMin; duration <= videoDurationWindowMax; duration++) {
                    // 如果cache中没有对应时长的视频,则查询数据库
                    if (!cachedVideoIdByDuration.containsKey(duration)) {
                        List<VideoInfoEntity> videoInfosByDuration = (new VideoInfoDao())
                                .getVideoInfoByDuration(duration);
                        Set<String> videoIdSet = new HashSet<String>();
                        for (VideoInfoEntity videoInfoEnt : videoInfosByDuration) {
                            videoIdSet.add(videoInfoEnt.getVideoId());
                            // 将视频文本信息存入cache
                            if (!cachedVideoText.containsKey(videoInfoEnt.getVideoId())) {
                                cachedVideoText.put(videoInfoEnt.getVideoId(),
                                        videoInfoEnt.getTitle());
                            }
                        }
                        // 存入cache
                        cachedVideoIdByDuration.put(duration, videoIdSet);
                        //                        logger.info("Cache duration:" + duration + ", size:" + videoIdSet.size());
                        // 存入待比较视频列表
                        comparedVideoIdSet.addAll(videoIdSet);
                    }
                    // 如果cache中有对应时长的视频,则直接查询内存的Map
                    else {
                        comparedVideoIdSet.addAll(cachedVideoIdByDuration.get(duration));
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

                    Analyzer analyzer = new IKAnalyzer(true);

                    // 如果缓存中没有compare视频的文本信息,则将新查询到的视频标签存入缓存
                    if (!cachedVideoText.containsKey(comparedVideoId)) {
                        VideoInfoEntity videoInfo = (new VideoInfoDao())
                                .getVideoInfoById(comparedVideoId);
                        cachedVideoText.put(comparedVideoId, videoInfo.getTitle());
                        comparedVideoText = videoInfo.getTitle();
                    }
                    // 如果缓存里有compare视频的文本信息,则查询缓存
                    else {
                        comparedVideoText = cachedVideoText.get(comparedVideoId);
                    }

                    // 如果query或者compare视频的文本信息为空,则继续比较下个视频
                    if (null == queryVideoText || null == comparedVideoText) {
                        logger.info("query或者compare视频的文本信息为空: "
                                + queryVideoInfo.getVideoId()
                                + " with " + comparedVideoId);
                        continue;
                    }

                    //                    logger.info("Compare " + queryVideoText + " with " + comparedVideoText);

                    List<String> querySplitText = MyStringUtils.wordSegment(queryVideoText);
                    List<String> comparedSplitText = MyStringUtils.wordSegment(comparedVideoText);

                    // 如果query或者compare视频分词结果为空,则继续比较下个视频
                    if (querySplitText.isEmpty() || comparedSplitText.isEmpty()) {
                        logger.info("query或者compare视频分词结果为空: " + queryVideoInfo.getVideoId()
                                + " with " + comparedVideoId);
                        continue;
                    }

                    float queryVScompared = (float) 0.0; // query与compare逐词比较的相似度
                    float comparedVSquery = (float) 0.0; // compare与query逐词比较的相似度

                    int similarTermNum = 0;
                    // 计算query与compare相同的term数量占query总term的比例
                    for (int i = 0; i < querySplitText.size(); i++) {
                        int minIndex = (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) >= 0 ? (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW)
                                : 0;
                        int maxIndex = (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) < comparedSplitText
                                .size() ? (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW)
                                : (comparedSplitText.size() - 1);
                        for (int j = minIndex; j < maxIndex + 1; j++) {
                            if (querySplitText.get(i).equals(comparedSplitText.get(j))) {
                                //                                logger.info("Similar term: " + querySplitText.get(i));
                                similarTermNum++;
                                break;
                            }
                        }
                    }
                    queryVScompared = (float) similarTermNum / (float) querySplitText.size();

                    // 计算compare与query相同的term数量占compare总term的比例
                    similarTermNum = 0;
                    for (int i = 0; i < comparedSplitText.size(); i++) {
                        int minIndex = (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) >= 0 ? (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW)
                                : 0;
                        int maxIndex = (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) < querySplitText
                                .size() ? (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW)
                                : (querySplitText.size() - 1);
                        for (int j = minIndex; j < maxIndex + 1; j++) {
                            if (comparedSplitText.get(i).equals(querySplitText.get(j))) {
                                //                                logger.info("Similar term: " + comparedSplitText.get(i));
                                similarTermNum++;
                                break;
                            }
                        }
                    }
                    comparedVSquery = (float) similarTermNum / (float) comparedSplitText.size();

                    // 调和相似度
                    float harmonicSimilarity = queryVScompared * comparedVSquery
                            / (queryVScompared + comparedVSquery);
                    // 如果相似度大于阈值,存入相似列表
                    if (harmonicSimilarity >= Const.STORM_CONFIG.TEXT_SIMILARITY_THRESHOLD) {
                        TextSimilarVideo textSimilarVideo = new TextSimilarVideo(comparedVideoId,
                                harmonicSimilarity);
                        textSimilarVideoList.add(textSimilarVideo);
                        //                        logger.info("Text Similar video: " + queryVideoInfo.getVideoId() + " and "
                        //                                    + comparedVideoId);
                    }
                }
                // 按照距离从小到大进行排序
                Collections.sort(textSimilarVideoList, new TextSimilarVideo());
                for (TextSimilarVideo textSimilarVideo : textSimilarVideoList) {
                    logger.info("Text similar video: " + textSimilarVideo.getVideoId()
                            + ", similarity: " + textSimilarVideo.getTextSimilarity());
                }
                logger.info("Text similar video list size: " + textSimilarVideoList.size());
                // 在控制信息中加入相似视频列表,有可能为空!
                String textSimVideoListStr = (new Gson()).toJson(textSimilarVideoList);
                ctrlMsg.put("textSimilarVideoList", textSimVideoListStr);
            }
            // detection task,两个视频,比较它们的相似度
            else if (Const.STORM_CONFIG.DETECTION_TASK_FLAG.equals(taskType)) {
                // 获得控制信息中第一个视频的元数据
                String queryVideoInfoStr = ctrlMsg.get("queryVideo");
                VideoInfoEntity queryVideoInfo = (new Gson()).fromJson(queryVideoInfoStr,
                        VideoInfoEntity.class);
                String queryVideoText = queryVideoInfo.getTitle();
                // 获得控制信息中第二个视频的元数据
                String queryVideoInfoStr2 = ctrlMsg.get("queryVideo2");
                VideoInfoEntity queryVideoInfo2 = (new Gson()).fromJson(queryVideoInfoStr2,
                        VideoInfoEntity.class);
                String queryVideoText2 = queryVideoInfo2.getTitle();

                // 如果两个query视频的文本信息为空,则文本相似度设为0,
                if (null == queryVideoText || null == queryVideoText2) {
                    ctrlMsg.put("textSimilarity", Float.toString((float) 0.0));
                    return;
                }

                List<String> querySplitText = MyStringUtils.wordSegment(queryVideoText);
                List<String> query2SplitText = MyStringUtils.wordSegment(queryVideoText2);

                // 如果两个query视频分词结果为空,则文本相似度设为0,
                if (querySplitText.isEmpty() || query2SplitText.isEmpty()) {
                    ctrlMsg.put("textSimilarity", Float.toString((float) 0.0));
                    return;
                }

                float query1VS2 = (float) 0.0; // query1与query2逐词比较的相似度
                float query2VS1 = (float) 0.0; // query2与query1逐词比较的相似度

                int samerTermNum = 0;
                // 计算query与compare相同的term数量占query总term的比例
                for (int i = 0; i < querySplitText.size(); i++) {
                    int minIndex = (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) >= 0 ? (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW)
                            : 0;
                    int maxIndex = (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) < query2SplitText
                            .size() ? (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) : (query2SplitText
                            .size() - 1);
                    for (int j = minIndex; j < maxIndex + 1; j++) {
                        if (querySplitText.get(i).equals(query2SplitText.get(j))) {
                            samerTermNum++;
                            break;
                        }
                    }
                }
                query1VS2 = (float) samerTermNum / (float) querySplitText.size();

                // 计算compare与query相同的term数量占compare总term的比例
                samerTermNum = 0;
                for (int i = 0; i < query2SplitText.size(); i++) {
                    int minIndex = (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) >= 0 ? (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW)
                            : 0;
                    int maxIndex = (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) < querySplitText
                            .size() ? (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) : (querySplitText
                            .size() - 1);
                    for (int j = minIndex; j < maxIndex + 1; j++) {
                        if (query2SplitText.get(i).equals(querySplitText.get(j))) {
                            samerTermNum++;
                            break;
                        }
                    }
                }
                query2VS1 = (float) samerTermNum / (float) query2SplitText.size();

                // 调和相似度
                float harmonicSimilarity = query1VS2 * query2VS1 / (query1VS2 + query2VS1);
                System.out.println(harmonicSimilarity);
                // 放入文本相似度,输出
                ctrlMsg.put("textSimilarity", Float.toString(harmonicSimilarity));
            }

            //Global signature
            if (Const.STORM_CONFIG.RETRIEVAL_TASK_FLAG.equals(taskType)) {
                String videoInfoStr = ctrlMsg.get("queryVideo");
                VideoInfoEntity videoInfoEnt = (new Gson()).fromJson(videoInfoStr,
                        VideoInfoEntity.class);
                List<KeyFrameEntity> keyframeList = (new KeyFrameDao())
                        .getKeyFrameByVideoId(videoInfoEnt.getVideoId());
                // 如果该视频没有对应的关键帧信息,则放弃输出
                if (null == keyframeList || keyframeList.isEmpty()) {
                    return;
                }
                Collections.sort(keyframeList, new KeyFrameEntity());
                String keyframeListStr = (new Gson()).toJson(keyframeList);
                ctrlMsg.put("keyframeList", keyframeListStr);
                logger.info("put keyframeList: " + keyframeListStr);
                HSVSigEntity hsvSignature = GlobalSigGenerator.generate(keyframeList);
                VideoHSVSigEntity videoHsvSig = new VideoHSVSigEntity(videoInfoEnt.getVideoId(),
                        hsvSignature);
                String newGsonStr = (new Gson()).toJson(videoHsvSig);
                // 在控制信息中加入新字段
                ctrlMsg.put("globalSignature", newGsonStr);
                logger.info("put globalSignature: " + newGsonStr);
            }
            // detection任务,两个query视频
            else if (Const.STORM_CONFIG.DETECTION_TASK_FLAG.equals(taskType)) {
                String videoInfoStr = ctrlMsg.get("queryVideo");
                VideoInfoEntity videoInfoEnt = (new Gson()).fromJson(videoInfoStr,
                        VideoInfoEntity.class);
                List<KeyFrameEntity> keyframeList = (new KeyFrameDao())
                        .getKeyFrameByVideoId(videoInfoEnt.getVideoId());
                // 如果该视频没有对应的关键帧信息,则放弃输出,这种情况下认为这两个视频不相似
                if (null == keyframeList || keyframeList.isEmpty()) {
                    return;
                }
                Collections.sort(keyframeList, new KeyFrameEntity());
                String keyframeListStr = (new Gson()).toJson(keyframeList);
                ctrlMsg.put("keyframeList", keyframeListStr);
                HSVSigEntity hsvSignature = GlobalSigGenerator.generate(keyframeList);
                // 如果生成全局标签失败,则放弃输出
                if (null == hsvSignature) {
                    return;
                }
                VideoHSVSigEntity videoHsvSig = new VideoHSVSigEntity(videoInfoEnt.getVideoId(),
                        hsvSignature);
                String newGsonStr = (new Gson()).toJson(videoHsvSig);
                // 在控制信息中加入第一个query视频的全局标签
                ctrlMsg.put("globalSignature", newGsonStr);
                logger.info("put globalSignature: " + newGsonStr);
                String videoInfoStr2 = ctrlMsg.get("queryVideo2");
                VideoInfoEntity videoInfoEnt2 = (new Gson()).fromJson(videoInfoStr2,
                        VideoInfoEntity.class);
                List<KeyFrameEntity> keyframeList2 = (new KeyFrameDao())
                        .getKeyFrameByVideoId(videoInfoEnt2.getVideoId());
                // 如果该视频没有对应的关键帧信息,则放弃输出,这种情况下认为这两个视频不相似
                if (null == keyframeList2 || keyframeList2.isEmpty()) {
                    return;
                }
                Collections.sort(keyframeList2, new KeyFrameEntity());
                String keyframeListStr2 = (new Gson()).toJson(keyframeList2);
                ctrlMsg.put("keyframeList2", keyframeListStr2);
                HSVSigEntity hsvSignature2 = GlobalSigGenerator.generate(keyframeList2);
                // 如果生成全局标签失败,则放弃输出
                if (null == hsvSignature2) {
                    return;
                }
                VideoHSVSigEntity videoHsvSig2 = new VideoHSVSigEntity(videoInfoEnt2.getVideoId(),
                        hsvSignature2);
                String newGsonStr2 = (new Gson()).toJson(videoHsvSig2);
                // 在控制信息中加入第二个query视频的全局标签
                ctrlMsg.put("globalSignature2", newGsonStr2);
                logger.info("put globalSignature2: " + newGsonStr2);
            }

            //GlobalSigDistance
            // retrieval任务,一个query视频
            if (Const.STORM_CONFIG.RETRIEVAL_TASK_FLAG.equals(taskType)) {
                // 获得控制信息中检索视频元数据
                String queryVideoInfoStr = ctrlMsg.get("queryVideo");
                VideoInfoEntity queryVideoInfo = (new Gson()).fromJson(queryVideoInfoStr,
                        VideoInfoEntity.class);

                // 待比较视频的id集合(唯一集合),根据视频id即可以在数据库找到该视频的全局标签
                Set<String> comparedVideoIdSet = new HashSet<String>();
                // 如果采用filter-and-refine的策略,则将之前文本相似度处理之后的相似视频作为比较集
                if (Const.STORM_CONFIG.IS_FILTER_AND_REFINE
                        && ctrlMsg.containsKey("textSimilarVideoList")) {
                    // 解析文本内容相似的视频列表
                    List<TextSimilarVideo> textSimilarVideoList = new ArrayList<TextSimilarVideo>();
                    String textSimilarVideoListStr = ctrlMsg.get("textSimilarVideoList");
                    Type textSimilarVideoListType = new TypeToken<List<TextSimilarVideo>>() {
                    }.getType();
                    textSimilarVideoList = (new Gson()).fromJson(textSimilarVideoListStr,
                            textSimilarVideoListType);
                    for (TextSimilarVideo textSimilarVideo : textSimilarVideoList) {
                        comparedVideoIdSet.add(textSimilarVideo.getVideoId());
                    }
                }
                // 如果不采用filter-and-refine的策略,则根据视频时长选取合适的视频
                else {
                    // 待检索视频的时长
                    int queryVideoDuration = queryVideoInfo.getDuration();
                    // 计算视频时长比较窗口的大小
                    int videoDurationWindowMin = queryVideoDuration
                            - Const.STORM_CONFIG.VIDEO_DURATION_WINDOW;
                    if (videoDurationWindowMin <= 0) {
                        videoDurationWindowMin = 1;
                    }
                    int videoDurationWindowMax = queryVideoDuration
                            + Const.STORM_CONFIG.VIDEO_DURATION_WINDOW;
                    for (int duration = videoDurationWindowMin; duration <= videoDurationWindowMax; duration++) {
                        // 如果cache中没有对应时长的视频,则查询数据库
                        if (!cachedVideoIdByDuration.containsKey(duration)) {
                            List<VideoInfoEntity> videoInfosByDuration = (new VideoInfoDao())
                                    .getVideoInfoByDuration(duration);
                            Set<String> videoIdSet = new HashSet<String>();
                            for (VideoInfoEntity videoInfoEnt : videoInfosByDuration) {
                                videoIdSet.add(videoInfoEnt.getVideoId());
                            }
                            if (videoIdSet.isEmpty()) {
                                // 存入cache
                                cachedVideoIdByDuration.put(duration, videoIdSet);
                                //                            logger.info("Cache duration:" + duration + ", size:"
                                //                                        + videoIdSet.size());
                            }
                            // 存入待比较视频列表
                            comparedVideoIdSet.addAll(videoIdSet);
                        }
                        // 如果cache中有对应时长的视频,则直接查询内存的Map
                        else {
                            comparedVideoIdSet.addAll(cachedVideoIdByDuration.get(duration));
                        }
                    }
                }

                // 获得检索视频的全局标签
                String queryGlobalSigStr = ctrlMsg.get("globalSignature");
                VideoHSVSigEntity queryVideoHsvSig = (new Gson()).fromJson(queryGlobalSigStr,
                        VideoHSVSigEntity.class);

                // 输出结果,保存相似的视频
                List<GlobalSimilarVideo> globalSimilarVideoList = new ArrayList<GlobalSimilarVideo>();

                // 依次比较compare视频和query视频
                for (String comparedVideoId : comparedVideoIdSet) {
                    // 如果为检索视频本身，则跳过
                    if (comparedVideoId.equals(queryVideoInfo.getVideoId())) {
                        continue;
                    }
                    VideoHSVSigEntity comparedVideoHsvSig = null;

                    // 如果缓存中没有compare视频的HSV标签,则将新查询到的视频标签存入缓存
                    if (!cachedHSVSignature.containsKey(comparedVideoId)) {
                        comparedVideoHsvSig = (new HSVSignatureDao())
                                .getVideoHSVSigById(comparedVideoId);
                        // 如果数据库中没有视频对应的全局标签,则处理下个待比较的视频
                        if (null == comparedVideoHsvSig) {
                            logger
                                    .info("During comparing, no signature found in database, videoId: "
                                            + comparedVideoId);
                            continue;
                        }
                        cachedHSVSignature.put(comparedVideoId, comparedVideoHsvSig.getSig());
                        //                                logger.info("cache hsv signature, videoId: "
                        //                                            + comparedVideoId + ", duration: "
                        //                                            + duration);
                    }
                    // 如果缓存中有compare视频的HSV标签,则查询缓存
                    else {
                        comparedVideoHsvSig = new VideoHSVSigEntity(comparedVideoId,
                                cachedHSVSignature.get(comparedVideoId));
                    }
                    float euclideanDistance = SigSim.getEuclideanDistance(
                            queryVideoHsvSig.getSig(), comparedVideoHsvSig.getSig());
                    //                    logger.info("euclideanDistance: " + euclideanDistance + ", queryVideoId: "
                    //                                + queryVideoInfo.getVideoId() + ", comparedVideoId"
                    //                                + comparedVideoId);
                    if (euclideanDistance <= Const.STORM_CONFIG.GLOBALSIG_EUCLIDEAN_THRESHOLD) {
                        globalSimilarVideoList.add(new GlobalSimilarVideo(comparedVideoId,
                                euclideanDistance));
                    }
                }

                // 按照距离从小到大进行排序
                Collections.sort(globalSimilarVideoList, new GlobalSimilarVideo());

                for (GlobalSimilarVideo globalSimilarVideo : globalSimilarVideoList) {
                    logger.info("Global similar video: " + globalSimilarVideo.getVideoId()
                            + ", distance: " + globalSimilarVideo.getGlobalSigEucliDistance());
                }
                logger.info("Global similar video list size: " + globalSimilarVideoList.size());

                //                logger.info("Similar video size: " + globalSimilarVideoList.size());
                //                for (GlobalSimilarVideo similarVideo : globalSimilarVideoList) {
                //                    logger.info("Similar video, videoId: " + similarVideo.getVideoId()
                //                                + ", distance: " + similarVideo.getGlobalSigEucliDistance());
                //                }

                // 在控制信息中加入相似视频列表,有可能为空!
                String globalSimVideoListStr = (new Gson()).toJson(globalSimilarVideoList);
                ctrlMsg.put("globalSimilarVideoList", globalSimVideoListStr);
            }
            // detection task,两个视频,比较它们的相似度
            else if (Const.STORM_CONFIG.DETECTION_TASK_FLAG.equals(taskType)) {
                // 获得控制信息中第一个视频的元数据
                String queryVideoInfoStr = ctrlMsg.get("queryVideo");
                VideoInfoEntity queryVideoInfo = (new Gson()).fromJson(queryVideoInfoStr,
                        VideoInfoEntity.class);
                // 获得待比较的第一个视频的全局标签
                String queryGlobalSigStr = ctrlMsg.get("globalSignature");
                VideoHSVSigEntity queryVideoHsvSig = (new Gson()).fromJson(queryGlobalSigStr,
                        VideoHSVSigEntity.class);
                // 获得控制信息中第二个视频的元数据
                String queryVideoInfoStr2 = ctrlMsg.get("queryVideo2");
                VideoInfoEntity queryVideoInfo2 = (new Gson()).fromJson(queryVideoInfoStr2,
                        VideoInfoEntity.class);
                // 获得待比较的第二个视频的全局标签
                String queryGlobalSigStr2 = ctrlMsg.get("globalSignature2");
                VideoHSVSigEntity queryVideoHsvSig2 = (new Gson()).fromJson(queryGlobalSigStr2,
                        VideoHSVSigEntity.class);
                // 计算全局标签距离
                float euclideanDistance = SigSim.getEuclideanDistance(
                        queryVideoHsvSig.getSig(), queryVideoHsvSig2.getSig());
                ctrlMsg.put("globalDistance", Float.toString(euclideanDistance));
                //                logger.info("globalDistance: " + euclideanDistance + ", queryVideo1: "
                //                            + queryVideoInfo.getVideoId() + ", queryVideo2: "
                //                            + queryVideoInfo2.getVideoId());
            }

            if (Const.STORM_CONFIG.IS_DEBUG) {
                long taskEndtTime = System.currentTimeMillis();
                logger.info("cachedVideoInfos size: " + cachedVideoIdByDuration.size());
                logger.info("cachedText size: " + cachedVideoText.size());
                logger.info("cachedHSVSignature size: " + cachedHSVSignature.size());

                logger.info("Task time: " + (taskEndtTime - taskStartTime));
                continue;
            }

            //local feature
            // retrieval任务,一个query视频
            if (Const.STORM_CONFIG.RETRIEVAL_TASK_FLAG.equals(taskType)) {
                String keyframeListStr = ctrlMsg.get("keyframeList");
                logger.info(keyframeListStr);
                Type keyframeListType = new TypeToken<ArrayList<KeyFrameEntity>>() {
                }.getType();
                List<KeyFrameEntity> keyframeList = (new Gson()).fromJson(keyframeListStr,
                        keyframeListType);
                // 保存视频的SIFT标签,list每个元素为一个帧的SIFT关键点
                List<List<KDFeaturePoint>> keyPoints = new ArrayList<List<KDFeaturePoint>>();
                for (int i = 0; i < keyframeList.size(); i++) {
                    KeyFrameEntity keyframeEnt = keyframeList.get(i);
                    String keyframeFile = Const.CC_WEB_VIDEO.KEYFRAME_PATH_PREFIX
                            + Integer.parseInt(keyframeEnt.getVideoId()) / 100 + "\\"
                            + keyframeEnt.getKeyFrameName();
                    try {
                        BufferedImage img = ImageIO.read(new File(keyframeFile));
                        RenderImage ri = new RenderImage(img);
                        SIFT sift = new SIFT();
                        sift.detectFeatures(ri.toPixelFloatArray(null));
                        List<KDFeaturePoint> al = sift.getGlobalKDFeaturePoints();
                        keyPoints.add(al);
                    } catch (IOException e) {
                        logger.error("IO error when read image: " + keyframeFile, e);
                    }
                }
                // 在控制信息中加入局部关键点,可能为空!
                String keyPointsStr = (new Gson()).toJson(keyPoints);
                ctrlMsg.put("localSignature", keyPointsStr);
                logger.info("put localSignature: " + keyPointsStr);
            } else if (Const.STORM_CONFIG.DETECTION_TASK_FLAG.equals(taskType)) {
                String keyframeListStr = ctrlMsg.get("keyframeList");
                logger.info("keyframeList: " + keyframeListStr);
                Type keyframeListType = new TypeToken<ArrayList<KeyFrameEntity>>() {
                }.getType();
                List<KeyFrameEntity> keyframeList = (new Gson()).fromJson(keyframeListStr,
                        keyframeListType);
                List<List<KDFeaturePoint>> keyPoints = new ArrayList<List<KDFeaturePoint>>();
                for (int i = 0; i < keyframeList.size(); i++) {
                    KeyFrameEntity keyframeEnt = keyframeList.get(i);
                    String keyframeFile = Const.CC_WEB_VIDEO.KEYFRAME_PATH_PREFIX
                            + Integer.parseInt(keyframeEnt.getVideoId()) / 100 + "\\"
                            + keyframeEnt.getKeyFrameName();
                    try {
                        BufferedImage img = ImageIO.read(new File(keyframeFile));
                        RenderImage ri = new RenderImage(img);
                        SIFT sift = new SIFT();
                        sift.detectFeatures(ri.toPixelFloatArray(null));
                        List<KDFeaturePoint> al = sift.getGlobalKDFeaturePoints();
                        keyPoints.add(al);
                    } catch (IOException e) {
                        logger.error("IO error when read image: " + keyframeFile, e);
                    }
                }
                // 在控制信息中加入第一个视频的局部关键点,可能为空!
                String keyPointsStr = (new Gson()).toJson(keyPoints);
                ctrlMsg.put("localSignature", keyPointsStr);
                logger.info("put localSignature: " + keyPointsStr);

                String keyframeListStr2 = ctrlMsg.get("keyframeList2");
                logger.info("keyframeList2: " + keyframeListStr2);
                List<KeyFrameEntity> keyframeList2 = (new Gson()).fromJson(keyframeListStr2,
                        keyframeListType);
                List<List<KDFeaturePoint>> keyPoints2 = new ArrayList<List<KDFeaturePoint>>();
                for (int i = 0; i < keyframeList2.size(); i++) {
                    KeyFrameEntity keyframeEnt = keyframeList2.get(i);
                    String keyframeFile = Const.CC_WEB_VIDEO.KEYFRAME_PATH_PREFIX
                            + Integer.parseInt(keyframeEnt.getVideoId()) / 100 + "\\"
                            + keyframeEnt.getKeyFrameName();
                    try {
                        BufferedImage img = ImageIO.read(new File(keyframeFile));
                        RenderImage ri = new RenderImage(img);
                        SIFT sift = new SIFT();
                        sift.detectFeatures(ri.toPixelFloatArray(null));
                        List<KDFeaturePoint> al = sift.getGlobalKDFeaturePoints();
                        keyPoints2.add(al);
                    } catch (IOException e) {
                        logger.error("IO error when read image: " + keyframeFile, e);
                    }
                }
                // 在控制信息中加入第二个视频的局部关键点,可能为空!
                String keyPointsStr2 = (new Gson()).toJson(keyPoints2);
                ctrlMsg.put("localSignature2", keyPointsStr2);
                logger.info("put localSignature2: " + keyPointsStr2);
            }

            //local similarity
            // retrieval任务,一个query视频
            if (Const.STORM_CONFIG.RETRIEVAL_TASK_FLAG.equals(taskType)) {
                // query视频
                String queryVideoInfoStr = ctrlMsg.get("queryVideo");
                VideoInfoEntity queryVideoInfo = (new Gson()).fromJson(queryVideoInfoStr,
                        VideoInfoEntity.class);

                // 待比较视频的id集合(唯一集合),根据视频id即可以找到该视频的SIFT标签文件
                Set<String> comparedVideoIdSet = new HashSet<String>();
                // 如果采用filter-and-refine的策略,则将之前全局特征处理之后的相似视频作为比较集
                if (Const.STORM_CONFIG.IS_FILTER_AND_REFINE
                        && ctrlMsg.containsKey("globalSimilarVideoList")) {
                    // 解析全局特征相似的视频列表
                    List<GlobalSimilarVideo> globalSimilarVideoList = new ArrayList<GlobalSimilarVideo>();
                    String globalSimilarVideoListStr = ctrlMsg.get("globalSimilarVideoList");
                    Type globalSimilarVideoListType = new TypeToken<List<GlobalSimilarVideo>>() {
                    }.getType();
                    globalSimilarVideoList = (new Gson()).fromJson(globalSimilarVideoListStr,
                            globalSimilarVideoListType);
                    for (GlobalSimilarVideo globalSimilarVideo : globalSimilarVideoList) {
                        // 如果全局标签的距离小于某个可信的阈值,则不用比较该视频的局部标签
                        if (globalSimilarVideo.getGlobalSigEucliDistance() > Const.STORM_CONFIG.GLOBALSIG_EUCLIDEAN_TRUST_THRESHOLD)
                            comparedVideoIdSet.add(globalSimilarVideo.getVideoId());
                    }
                }
                // 如果采用filter-and-refine的策略,没有全局特征,则将之前文本相似度处理之后的相似视频作为比较集
                else if (Const.STORM_CONFIG.IS_FILTER_AND_REFINE
                        && ctrlMsg.containsKey("textSimilarVideoList")) {
                    // 解析文本内容相似的视频列表
                    List<TextSimilarVideo> textSimilarVideoList = new ArrayList<TextSimilarVideo>();
                    String textSimilarVideoListStr = ctrlMsg.get("textSimilarVideoList");
                    Type textSimilarVideoListType = new TypeToken<List<TextSimilarVideo>>() {
                    }.getType();
                    textSimilarVideoList = (new Gson()).fromJson(textSimilarVideoListStr,
                            textSimilarVideoListType);
                    for (TextSimilarVideo textSimilarVideo : textSimilarVideoList) {
                        comparedVideoIdSet.add(textSimilarVideo.getVideoId());
                    }
                }
                // 如果不采用filter-and-refine的策略,则根据视频时长选取合适的视频
                else {
                    // 待检索视频的时长
                    int queryVideoDuration = queryVideoInfo.getDuration();
                    // 计算视频时长比较窗口的大小
                    int videoDurationWindowMin = queryVideoDuration
                            - Const.STORM_CONFIG.VIDEO_DURATION_WINDOW;
                    if (videoDurationWindowMin <= 0) {
                        videoDurationWindowMin = 1;
                    }
                    int videoDurationWindowMax = queryVideoDuration
                            + Const.STORM_CONFIG.VIDEO_DURATION_WINDOW;
                    for (int duration = videoDurationWindowMin; duration <= videoDurationWindowMax; duration++) {
                        // 如果cache中没有对应时长的视频,则查询数据库
                        if (!cachedVideoIdByDuration.containsKey(duration)) {
                            List<VideoInfoEntity> videoInfosByDuration = (new VideoInfoDao())
                                    .getVideoInfoByDuration(duration);
                            Set<String> videoIdSet = new HashSet<String>();
                            for (VideoInfoEntity videoInfoEnt : videoInfosByDuration) {
                                videoIdSet.add(videoInfoEnt.getVideoId());
                            }
                            // 存入cache
                            cachedVideoIdByDuration.put(duration, videoIdSet);
                            // 存入待比较视频列表
                            comparedVideoIdSet.addAll(videoIdSet);
                        }
                        // 如果cache中有对应时长的视频,则直接查询内存的Map
                        else {
                            comparedVideoIdSet.addAll(cachedVideoIdByDuration.get(duration));
                        }
                    }
                }

                // 获得query视频的局部标签
                String queryLocalSigStr = ctrlMsg.get("localSignature");
                // query视频SIFT标签的type
                Type queryLocalSigType = new TypeToken<List<List<KDFeaturePoint>>>() {
                }.getType();
                // query视频的标签,list大小为query视频帧图像的数量,内层list大小为SIFT特征点的个数
                List<List<KDFeaturePoint>> queryLocalSigs = (new Gson()).fromJson(queryLocalSigStr,
                        queryLocalSigType);

                if (queryLocalSigs.isEmpty()) {
                    logger.info("Query video's local signature is null: "
                            + queryVideoInfo.getVideoId());
                    continue;
                }

                // 保存局部特征相似的视频
                List<LocalSimilarVideo> localSimilarVideoList = new ArrayList<LocalSimilarVideo>();

                // 依次和视频时长在窗口内的视频比较
                for (String comparedVideoId : comparedVideoIdSet) {
                    System.out.println("comparedVideo: " + comparedVideoId);
                    // 指定时长的视频的SIFT标签文件
                    String comparedVideoSIFTFilePath = Const.CC_WEB_VIDEO.SIFT_SIGNATURE_PATH_PREFIX
                            + Integer.parseInt(comparedVideoId)
                            / 100
                            + "\\" + comparedVideoId + ".txt";
                    File comparedVideoSIFTFile = new File(comparedVideoSIFTFilePath);
                    if (!comparedVideoSIFTFile.exists()) {
                        continue;
                    }
                    try {
                        BufferedReader reader = new BufferedReader(new FileReader(
                                comparedVideoSIFTFile));
                        String line = reader.readLine();
                        // 文件的一行代表一个compare视频
                        if (null != line) {
                            // compare视频的SIFT标签
                            VideoSIFTSigEntity comparedVideoLocalSig = (new Gson()).fromJson(line,
                                    VideoSIFTSigEntity.class);
                            if (null == comparedVideoLocalSig) {
                                continue;
                            }
                            // compare视频各个帧图像的SIFT标签
                            List<SIFTSigEntity> comparedKeyframeSigs = comparedVideoLocalSig
                                    .getSignature();
                            if (comparedKeyframeSigs.isEmpty()) {
                                continue;
                            }
                            // 记录相似的帧图像数量
                            int similarKeyframeNum = 0;
                            // i表示query视频的帧序号,依次将query视频的每个帧与compare视频进行比较
                            for (int i = 0; i < queryLocalSigs.size(); i++) {
                                // compare视频帧图像的比较窗口边界
                                int comparedFrameLeft = i
                                        - Const.STORM_CONFIG.FRAME_COMPARED_WINDOW;
                                if (comparedFrameLeft < 0) {
                                    comparedFrameLeft = 0;
                                }
                                int comparedFrameRight = i
                                        + Const.STORM_CONFIG.FRAME_COMPARED_WINDOW;
                                if (comparedFrameRight >= comparedKeyframeSigs.size()) {
                                    comparedFrameRight = comparedKeyframeSigs.size() - 1;
                                }

                                // j表示compare视频的帧序号
                                for (int j = comparedFrameLeft; j <= comparedFrameRight; j++) {
                                    if (queryLocalSigs.get(i).isEmpty()
                                            || comparedKeyframeSigs.get(j).getSig().isEmpty()) {
                                        continue;
                                    }
                                    // 比较query视频和compare视频
                                    try {
                                        List<Match> ms = MatchKeys.findMatchesBBF(queryLocalSigs
                                                .get(i), comparedKeyframeSigs.get(j).getSig());
                                        ms = MatchKeys.filterMore(ms);
                                        // 如果找到一个相似度大于阈值的帧,则开始比较query视频的下一个帧图像
                                        if ((float) ms.size()
                                                / (float) queryLocalSigs.get(i).size() >= Const.STORM_CONFIG.LOCALSIG_KEYFRAME_SIMILARITY_THRESHOLD) {
                                            similarKeyframeNum++;
                                            break;
                                        }
                                    } catch (IllegalArgumentException e) {
                                        logger.error("ComparedVideoId: " + comparedVideoId + ", "
                                                + e);
                                    }
                                }
                            }
                            float localSigSimilarity = (float) similarKeyframeNum
                                    / (float) queryLocalSigs.size();
                            //                            logger.info("localSigSimilarity: " + localSigSimilarity
                            //                                        + ", queryVideo: " + queryVideoInfo.getVideoId()
                            //                                        + ", comparedVideo: " + comparedVideoId);
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

                for (LocalSimilarVideo localSimilarVideo : localSimilarVideoList) {
                    logger.info("Local similar video: " + localSimilarVideo.getVideoId()
                            + ", similarity: " + localSimilarVideo.getLocalSigSimilarity());
                }
                logger.info("Local similar video list size: " + localSimilarVideoList.size());

                // 在控制信息中加入局部特征相似视频列表,有可能为空!
                String localSimilarVideoListStr = (new Gson()).toJson(localSimilarVideoList);
                ctrlMsg.put("localSimilarVideoList", localSimilarVideoListStr);
            } else if (Const.STORM_CONFIG.DETECTION_TASK_FLAG.equals(taskType)) {
                // 第一个视频的信息
                String queryVideoInfoStr = ctrlMsg.get("queryVideo");
                VideoInfoEntity queryVideoInfo = (new Gson()).fromJson(queryVideoInfoStr,
                        VideoInfoEntity.class);
                // 获得第一个视频的局部标签
                String queryLocalSigStr = ctrlMsg.get("localSignature");
                // 视频SIFT标签的type
                Type queryLocalSigType = new TypeToken<List<List<KDFeaturePoint>>>() {
                }.getType();
                // 第一个视频的标签,list大小为query视频帧图像的数量,内层list大小为SIFT特征点的个数
                List<List<KDFeaturePoint>> queryLocalSigs = (new Gson()).fromJson(queryLocalSigStr,
                        queryLocalSigType);

                // 第二个视频的信息
                String queryVideoInfoStr2 = ctrlMsg.get("queryVideo2");
                VideoInfoEntity queryVideoInfo2 = (new Gson()).fromJson(queryVideoInfoStr2,
                        VideoInfoEntity.class);
                // 获得第二个视频的局部标签
                String queryLocalSigStr2 = ctrlMsg.get("localSignature2");
                // 第二个视频的标签,list大小为视频帧图像的数量,内层list大小为SIFT特征点的个数
                List<List<KDFeaturePoint>> queryLocalSigs2 = (new Gson()).fromJson(
                        queryLocalSigStr2, queryLocalSigType);

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
                        // 比较query视频和compare视频
                        List<Match> ms = MatchKeys.findMatchesBBF(queryLocalSigs.get(i),
                                queryLocalSigs2.get(j));
                        ms = MatchKeys.filterMore(ms);
                        // 如果找到一个相似度大于阈值的帧,则开始比较query视频的下一个帧图像
                        if ((float) ms.size() / (float) queryLocalSigs.size() >= Const.STORM_CONFIG.LOCALSIG_KEYFRAME_SIMILARITY_THRESHOLD) {
                            similarKeyframeNum++;
                            break;
                        }
                    }
                }
                float localSigSimilarity = (float) similarKeyframeNum
                        / (float) queryLocalSigs.size();
                logger.info("local signature similarity: " + localSigSimilarity);
                // 在控制信息中加入局部特征的相似度
                ctrlMsg.put("localSimilarity", Float.toString(localSigSimilarity));
            }

            // retrieval任务,一个query视频
            if (Const.STORM_CONFIG.RETRIEVAL_TASK_FLAG.equals(taskType)) {
                // query视频
                String queryVideoInfoStr = ctrlMsg.get("queryVideo");
                VideoInfoEntity queryVideoInfo = (new Gson()).fromJson(queryVideoInfoStr,
                        VideoInfoEntity.class);
                String localSimilarVideoListStr = ctrlMsg.get("localSimilarVideoList");
                Type localSimilarVideoListType = new TypeToken<List<LocalSimilarVideo>>() {
                }.getType();
                List<LocalSimilarVideo> localSimilarVideoList = (new Gson()).fromJson(
                        localSimilarVideoListStr, localSimilarVideoListType);
                TaskEntity resultTask = new TaskEntity();
                resultTask.setTaskId(taskId);
                resultTask.setTaskType(taskType);
                List<String> resultBideoIdList = new ArrayList<String>();
                for (LocalSimilarVideo localSimilarVideo : localSimilarVideoList) {
                    resultBideoIdList.add(localSimilarVideo.getVideoId());
                }
                task.setVideoIdList(resultBideoIdList);
                task.setStatus("1");
                TaskResultDao taskResultDao = new TaskResultDao();
                taskResultDao.insert(task);
            }

            long taskEndtTime = System.currentTimeMillis();
            logger.info("cachedVideoInfos size: " + cachedVideoIdByDuration.size());
            logger.info("cachedText size: " + cachedVideoText.size());
            logger.info("cachedHSVSignature size: " + cachedHSVSignature.size());

            logger.info("Task time: " + (taskEndtTime - taskStartTime));
        }

    }
}
