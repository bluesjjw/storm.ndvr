/**
 * @Package cn.pku.net.db.storm.ndvr.localmode
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.localmode;

import java.lang.reflect.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;

import org.wltea.analyzer.lucene.IKAnalyzer;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import cn.pku.net.db.storm.ndvr.bolt.TextSimilarityRetrievalBolt;
import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.dao.HSVSignatureDao;
import cn.pku.net.db.storm.ndvr.dao.TaskResultDao;
import cn.pku.net.db.storm.ndvr.dao.VideoInfoDao;
import cn.pku.net.db.storm.ndvr.entity.HSVSigEntity;
import cn.pku.net.db.storm.ndvr.entity.TaskEntity;
import cn.pku.net.db.storm.ndvr.entity.TextSimilarVideo;
import cn.pku.net.db.storm.ndvr.entity.VideoHSVSigEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoInfoEntity;

/**
 * Description: Local test NDVR thread
 *
 * @author jeremyjiang
 * Created at 2016/5/12 18:35
 */
public class TestThread implements Runnable {
    private static final Logger logger = Logger.getLogger(TestThread.class);
    private TaskEntity          task   = new TaskEntity();

    /**
     * Instantiates a new Text thread.
     *
     * @param taskEnt the task
     */
    public TestThread(TaskEntity taskEnt) {
        super();
        this.task = taskEnt;
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {}

    /**
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        long                      taskStartTime           = System.currentTimeMillis();
        Map<Integer, Set<String>> cachedVideoIdByDuration = new ConcurrentHashMap<Integer, Set<String>>();    // 缓存视频数据,key为duration,value为视频元数据
        Map<String, String>       cachedVideoText    = new ConcurrentHashMap<String, String>();    // 缓存视频数据,key为视频id,value为视频文本信息
        Map<String, HSVSigEntity> cachedHSVSignature = new ConcurrentHashMap<String, HSVSigEntity>();    // 缓存视频的HSV全局标签,key为视频id,value为视频HSV全局标签
        VideoInfoDao          videoInfoDao      = new VideoInfoDao();
        List<VideoInfoEntity> zerovideoInfoList = videoInfoDao.getVideoInfoByDuration(0);    // 取出时长为0的视频(数据集中有些视频没有duration数据,我们设为0)

        if ((null != zerovideoInfoList) &&!zerovideoInfoList.isEmpty()) {
            Set<String> videoIdSet = new HashSet<String>();

            for (VideoInfoEntity videoInfoEnt : zerovideoInfoList) {
                videoIdSet.add(videoInfoEnt.getVideoId());

                if (null != videoInfoEnt.getTitle()) {
                    videoIdSet.add(videoInfoEnt.getVideoId());
                    cachedVideoText.put(videoInfoEnt.getVideoId(), videoInfoEnt.getTitle());    // 缓存时长为0的视频的HSV全局标签
                }

                VideoHSVSigEntity videoHsvSig = (new HSVSignatureDao()).getVideoHSVSigById(videoInfoEnt.getVideoId());

                // 如果数据库中没有该视频的HSV标签,则继续下一个视频
                if (null == videoHsvSig) {
                    continue;
                }

                cachedHSVSignature.put(videoHsvSig.getVideoId(), videoHsvSig.getSig());         // 缓存时长为0的视频的HSV全局标签
            }

            cachedVideoIdByDuration.put(0, videoIdSet);                                         // 将时长为0的视频缓存
        }

        logger.info("cachedVideoInfos size: " + cachedVideoIdByDuration.size());
        logger.info("cachedHSVSignature size: " + cachedHSVSignature.size());

        Map<String, String>   ctrlMsg       = new HashMap<String, String>();    // 控制信息,key为String,value为Gson生成的JSON字符串
        String                taskId        = task.getTaskId();
        String                taskType      = task.getTaskType();
        List<VideoInfoEntity> videoInfoList = new ArrayList<VideoInfoEntity>();

        // 取出task对应的query视频
        // 取出task对应的query视频id
        List<String> videoIdList = task.getVideoIdList();

        for (String videoId : videoIdList) {
            videoInfoList.add(videoInfoDao.getVideoInfoById(videoId));
        }

        if (Const.STORM_CONFIG.RETRIEVAL_TASK_FLAG.equals(taskType)) {
            VideoInfoEntity ent     = videoInfoList.get(0);
            Gson            gson    = new Gson();
            String          gsonStr = gson.toJson(ent);

            ctrlMsg.put("queryVideo", gsonStr);
            logger.info("put queryVideo: " + gsonStr);
        } else if (Const.STORM_CONFIG.DETECTION_TASK_FLAG.equals(taskType)) {
            for (VideoInfoEntity ent1 : videoInfoList) {
                for (VideoInfoEntity ent2 : videoInfoList) {

                    // 保证第一个视频的id小于第二个视频的id
                    if (Integer.parseInt(ent1.getVideoId()) < Integer.parseInt(ent2.getVideoId())) {
                        Gson   gson    = new Gson();
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

        // text similarity
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

                // 如果cache中没有对应时长的视频,则查询数据库
                if (!cachedVideoIdByDuration.containsKey(duration)) {
                    List<VideoInfoEntity> videoInfosByDuration = (new VideoInfoDao()).getVideoInfoByDuration(duration);
                    Set<String>           videoIdSet           = new HashSet<String>();

                    for (VideoInfoEntity videoInfoEnt : videoInfosByDuration) {
                        videoIdSet.add(videoInfoEnt.getVideoId());

                        // 将视频文本信息存入cache
                        if (!cachedVideoText.containsKey(videoInfoEnt.getVideoId())) {
                            cachedVideoText.put(videoInfoEnt.getVideoId(), videoInfoEnt.getTitle());
                        }
                    }

                    // 存入cache
                    cachedVideoIdByDuration.put(duration, videoIdSet);

                    // logger.info("Cache duration:" + duration + ", size:" + videoIdSet.size());
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
                String   comparedVideoText = null;
                Analyzer analyzer          = new IKAnalyzer(true);

                // 如果缓存中没有compare视频的文本信息,则将新查询到的视频标签存入缓存
                if (!cachedVideoText.containsKey(comparedVideoId)) {
                    VideoInfoEntity videoInfo = (new VideoInfoDao()).getVideoInfoById(comparedVideoId);

                    cachedVideoText.put(comparedVideoId, videoInfo.getTitle());
                    comparedVideoText = videoInfo.getTitle();
                }

                // 如果缓存里有compare视频的文本信息,则查询缓存
                else {
                    comparedVideoText = cachedVideoText.get(comparedVideoId);
                }

                // 如果query或者compare视频的文本信息为空,则继续比较下个视频
                if ((null == queryVideoText) || (null == comparedVideoText)) {

                    // logger.info("query或者compare视频的文本信息为空: " +
                    // queryVideoInfo.getVideoId()
                    // + " with " + comparedVideoId);
                    continue;
                }

                // logger.info("Compare " + queryVideoText + " with " + comparedVideoText);
                List<String> querySplitText    = TextSimilarityRetrievalBolt.getSplitText(queryVideoText);
                List<String> comparedSplitText = TextSimilarityRetrievalBolt.getSplitText(comparedVideoText);

                // 如果query或者compare视频分词结果为空,则继续比较下个视频
                if (querySplitText.isEmpty() || comparedSplitText.isEmpty()) {

                    // logger.info("query或者compare视频分词结果为空: " +
                    // queryVideoInfo.getVideoId() + " with "
                    // + comparedVideoId);
                    continue;
                }

                float queryVScompared = (float) 0.0;    // query与compare逐词比较的相似度
                float comparedVSquery = (float) 0.0;    // compare与query逐词比较的相似度
                int   similarTermNum  = 0;

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

                            // logger.info("Similar term: " + querySplitText.get(i));
                            similarTermNum++;

                            break;
                        }
                    }
                }

                queryVScompared = (float) similarTermNum / (float) querySplitText.size();

                // 计算compare与query相同的term数量占compare总term的比例
                similarTermNum = 0;

                for (int i = 0; i < comparedSplitText.size(); i++) {
                    int minIndex = (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) >= 0
                                   ? (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW)
                                   : 0;
                    int maxIndex = (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) < querySplitText.size()
                                   ? (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW)
                                   : (querySplitText.size() - 1);

                    for (int j = minIndex; j < maxIndex + 1; j++) {
                        if (comparedSplitText.get(i).equals(querySplitText.get(j))) {

                            // logger.info("Similar term: " + comparedSplitText.get(i));
                            similarTermNum++;

                            break;
                        }
                    }
                }

                comparedVSquery = (float) similarTermNum / (float) comparedSplitText.size();

                // 调和相似度
                float harmonicSimilarity = queryVScompared * comparedVSquery / (queryVScompared + comparedVSquery);

                // 如果相似度大于阈值,存入相似列表
                if (harmonicSimilarity >= Const.STORM_CONFIG.TEXT_SIMILARITY_THRESHOLD) {
                    TextSimilarVideo textSimilarVideo = new TextSimilarVideo(comparedVideoId, harmonicSimilarity);

                    textSimilarVideoList.add(textSimilarVideo);

                    // logger.info("Text Similar video: " + queryVideoInfo.getVideoId() + " and "
                    // + comparedVideoId);
                }
            }

            // 按照距离从小到大进行排序
            Collections.sort(textSimilarVideoList, new TextSimilarVideo());

            // for (TextSimilarVideo textSimilarVideo : textSimilarVideoList) {
            // logger.info("Text similar video: " + textSimilarVideo.getVideoId()
            // + ", similarity: " + textSimilarVideo.getTextSimilarity());
            // }
            // logger.info("Text similar video list size: " + textSimilarVideoList.size());
            // 在控制信息中加入相似视频列表,有可能为空!
            String textSimVideoListStr = (new Gson()).toJson(textSimilarVideoList);

            ctrlMsg.put("textSimilarVideoList", textSimVideoListStr);
            logger.info(textSimVideoListStr);
        }

        // detection task,两个视频,比较它们的相似度
        else if (Const.STORM_CONFIG.DETECTION_TASK_FLAG.equals(taskType)) {

            // 获得控制信息中第一个视频的元数据
            String          queryVideoInfoStr = ctrlMsg.get("queryVideo");
            VideoInfoEntity queryVideoInfo    = (new Gson()).fromJson(queryVideoInfoStr, VideoInfoEntity.class);
            String          queryVideoText    = queryVideoInfo.getTitle();

            // 获得控制信息中第二个视频的元数据
            String          queryVideoInfoStr2 = ctrlMsg.get("queryVideo2");
            VideoInfoEntity queryVideoInfo2    = (new Gson()).fromJson(queryVideoInfoStr2, VideoInfoEntity.class);
            String          queryVideoText2    = queryVideoInfo2.getTitle();

            // 如果两个query视频的文本信息为空,则文本相似度设为0,
            if ((null == queryVideoText) || (null == queryVideoText2)) {
                ctrlMsg.put("textSimilarity", Float.toString((float) 0.0));

                return;
            }

            List<String> querySplitText  = TextSimilarityRetrievalBolt.getSplitText(queryVideoText);
            List<String> query2SplitText = TextSimilarityRetrievalBolt.getSplitText(queryVideoText2);

            // 如果两个query视频分词结果为空,则文本相似度设为0,
            if (querySplitText.isEmpty() || query2SplitText.isEmpty()) {
                ctrlMsg.put("textSimilarity", Float.toString((float) 0.0));

                return;
            }

            float query1VS2    = (float) 0.0;    // query1与query2逐词比较的相似度
            float query2VS1    = (float) 0.0;    // query2与query1逐词比较的相似度
            int   samerTermNum = 0;

            // 计算query与compare相同的term数量占query总term的比例
            for (int i = 0; i < querySplitText.size(); i++) {
                int minIndex = (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) >= 0
                               ? (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW)
                               : 0;
                int maxIndex = (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) < query2SplitText.size()
                               ? (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW)
                               : (query2SplitText.size() - 1);

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
                int minIndex = (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) >= 0
                               ? (i - Const.STORM_CONFIG.TEXT_COMPARED_WINDOW)
                               : 0;
                int maxIndex = (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW) < querySplitText.size()
                               ? (i + Const.STORM_CONFIG.TEXT_COMPARED_WINDOW)
                               : (querySplitText.size() - 1);

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

        // 存入数据库
        if (Const.STORM_CONFIG.RETRIEVAL_TASK_FLAG.equals(taskType)) {

            // query视频
            String                 queryVideoInfoStr    = ctrlMsg.get("queryVideo");
            VideoInfoEntity        queryVideoInfo       = (new Gson()).fromJson(queryVideoInfoStr,
                                                                                VideoInfoEntity.class);
            String                 similarVideoListStr  = ctrlMsg.get("textSimilarVideoList");
            Type                   similarVideoListType = new TypeToken<List<TextSimilarVideo>>() {}
            .getType();
            List<TextSimilarVideo> similarVideoList     = (new Gson()).fromJson(similarVideoListStr,
                                                                                similarVideoListType);
            TaskEntity             task                 = new TaskEntity();

            task.setTaskId(taskId);
            task.setTaskType(taskType);

            List<String> resultVideoIdList = new ArrayList<String>();

            for (TextSimilarVideo similarVideo : similarVideoList) {
                resultVideoIdList.add(similarVideo.getVideoId());
            }

            task.setVideoIdList(resultVideoIdList);
            task.setStatus("1");
            task.setTimeStamp(Long.toString(System.currentTimeMillis()));

            TaskResultDao taskResultDao = new TaskResultDao();

            taskResultDao.insert(task);
        }

        if (Const.STORM_CONFIG.IS_DEBUG) {
            long taskEndtTime = System.currentTimeMillis();

            logger.info("cachedVideoInfos size: " + cachedVideoIdByDuration.size());
            logger.info("cachedText size: " + cachedVideoText.size());
            logger.info("cachedHSVSignature size: " + cachedHSVSignature.size());
            logger.info("Task time: " + (taskEndtTime - taskStartTime));
        }
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
