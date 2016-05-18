/**
 * Created by jeremyjiang on 2016/5/17.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.general;

import java.lang.reflect.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.dao.TaskDao;
import cn.pku.net.db.storm.ndvr.dao.TaskResultDao;
import cn.pku.net.db.storm.ndvr.entity.GlobalSimilarVideo;
import cn.pku.net.db.storm.ndvr.entity.TaskEntity;
import cn.pku.net.db.storm.ndvr.entity.TextSimilarVideo;
import cn.pku.net.db.storm.ndvr.entity.VideoInfoEntity;

/**
 * Description: Use textual and global visual feature, post-filtering strategy
 *
 * @author jeremyjiang
 * Created at 2016/5/17 10:11
 */
public class TextGlobalPostResultBolt extends BaseBasicBolt {
    private static final Logger              logger                = Logger.getLogger(TextGlobalPostResultBolt.class);
    private static Map<String, List<String>> taskResultMap         = new ConcurrentHashMap<String, List<String>>();    // 存储task执行结果
    private static Map<String, Integer>      taskSizeMap           = new ConcurrentHashMap<String, Integer>();    // 存储task的规模
    private static Map<String, Integer>      taskResultComparedMap = new ConcurrentHashMap<String, Integer>();    // 已经比较过的对数
    private static Map<String, Long>         taskStartTimeStampMap = new ConcurrentHashMap<String, Long>();    // task的开始时间

    /**
     * Declare output fields.
     *
     * @param declarer the declarer
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer) backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

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
        String              taskId          = input.getStringByField("taskId");
        String              taskType        = input.getStringByField("taskType");
        int                 fieldGroupingId = input.getIntegerByField("fieldGroupingId");
        Map<String, String> ctrlMsg         = (Map<String, String>) input.getValue(3);    // 控制信息

        // retrieval任务,一个query视频
        if (Const.STORM_CONFIG.RETRIEVAL_TASK_FLAG.equals(taskType)) {

            // query video
            // String queryVideoInfoStr = ctrlMsg.get("queryVideo");
            // VideoInfoEntity queryVideoInfo = (new Gson()).fromJson(queryVideoInfoStr,
            // VideoInfoEntity.class);
            // get textual similar video list
            String                 textSimilarVideoListStr  = ctrlMsg.get("textSimilarVideoList");
            Type                   textSimilarVideoListType = new TypeToken<List<TextSimilarVideo>>() {}
            .getType();
            List<TextSimilarVideo> textSimilarVideoList     = (new Gson()).fromJson(textSimilarVideoListStr,
                                                                                    textSimilarVideoListType);

            // get global visual similar video list
            String                   globalSimilarVideoListStr  = ctrlMsg.get("globalSimilarVideoList");
            Type                     globalSimilarVideoListType = new TypeToken<List<GlobalSimilarVideo>>() {}
            .getType();
            List<GlobalSimilarVideo> globalSimilarVideoList     = (new Gson()).fromJson(globalSimilarVideoListStr,
                                                                                        globalSimilarVideoListType);
            List<String> postVideoIdList = new ArrayList<String>();

            if (null != textSimilarVideoList) {

                // if both text similar list and global visual similar list contain the same video, then consider it as similar video
                for (TextSimilarVideo textSimilarVideo : textSimilarVideoList) {
                    String videoId = textSimilarVideo.getVideoId();

                    if ((null != globalSimilarVideoList) && globalSimilarVideoList.contains(videoId)) {
                        postVideoIdList.add(videoId);
                    }
                }
            }

            TaskEntity task = new TaskEntity();

            task.setTaskId(taskId);
            task.setTaskType(taskType);
            task.setVideoIdList(postVideoIdList);
            task.setStatus("1");

            long startTimeStamp = Long.parseLong(ctrlMsg.get("startTimeStamp"));

            task.setTimeStamp(Long.toString(System.currentTimeMillis() - startTimeStamp));

            TaskResultDao taskResultDao = new TaskResultDao();

            taskResultDao.insert(task);
        }
        // detection任务,两个query视频
        else if (Const.STORM_CONFIG.DETECTION_TASK_FLAG.equals(taskType)) {
            if (!taskResultMap.containsKey(taskId)) {
                TaskEntity task = (new TaskDao()).getTaskById(taskId);

                taskResultMap.put(taskId, task.getVideoIdList());
                taskSizeMap.put(taskId, task.getVideoIdList().size());
                taskResultComparedMap.put(taskId, 0);

                long startTimeStamp = Long.parseLong(ctrlMsg.get("startTimeStamp"));

                taskStartTimeStampMap.put(taskId, startTimeStamp);
            }

            String          queryVideoStr1 = ctrlMsg.get("queryVideo1");
            String          queryVideoStr2 = ctrlMsg.get("queryVideo2");
            VideoInfoEntity queryVideo1    = (new Gson()).fromJson(queryVideoStr1, VideoInfoEntity.class);
            VideoInfoEntity queryVideo2    = (new Gson()).fromJson(queryVideoStr2, VideoInfoEntity.class);

            // handle the global visual similarity
            if (ctrlMsg.containsKey("globalDistance")
                    && (Float.parseFloat(ctrlMsg.get("globalDistance"))
                        > Const.STORM_CONFIG.GLOBALSIG_EUCLIDEAN_THRESHOLD)) {
                List<String> videoIdList = taskResultMap.get(taskId);
                int          index1      = videoIdList.indexOf(queryVideo1.getVideoId());
                int          index2      = videoIdList.indexOf(queryVideo2.getVideoId());

                if ((index1 == -1) || (index2 == -1)) {}
                else if (index1 > index2) {
                    videoIdList.remove(index1);
                    logger.info(String.format("TaskId: %s, remove video: %s w.r.t. global visual similarity",
                                              taskId,
                                              index1));
                } else {
                    videoIdList.remove(index2);
                    logger.info(String.format("TaskId: %s, remove video: %s w.r.t. global visual similarity",
                                              taskId,
                                              index2));
                }

                taskResultMap.put(taskId, videoIdList);
            }

            // handle the textual similarity
            if (ctrlMsg.containsKey("textSimilarity")
                    && (Float.parseFloat(ctrlMsg.get("textSimilarity"))
                        > Const.STORM_CONFIG.TEXT_SIMILARITY_THRESHOLD)) {
                List<String> videoIdList = taskResultMap.get(taskId);
                int          index1      = videoIdList.indexOf(queryVideo1.getVideoId());
                int          index2      = videoIdList.indexOf(queryVideo2.getVideoId());

                if ((index1 == -1) || (index2 == -1)) {}
                else if (index1 > index2) {
                    videoIdList.remove(index1);
                    logger.info(String.format("TaskId: %s, remove video: %s w.r.t. textual similarity",
                                              taskId,
                                              index1));
                } else {
                    videoIdList.remove(index2);
                    logger.info(String.format("TaskId: %s, remove video: %s w.r.t. textual similarity ",
                                              taskId,
                                              index2));
                }
                taskResultMap.put(taskId, videoIdList);
            }

            int taskSize           = taskSizeMap.get(taskId);
            int totalComparedCount = (taskSize * (taskSize - 1));    // global visual + textual, both n(n-1)/2
            int taskResultCompared = taskResultComparedMap.get(taskId) + 1;

            if (totalComparedCount == taskResultCompared) {
                TaskEntity task = new TaskEntity();

                task.setTaskId(taskId);
                task.setTaskType(taskType);

                List<String> videoIdList = taskResultMap.get(taskId);

                task.setVideoIdList(videoIdList);
                task.setStatus("1");
                task.setTimeStamp(Long.toString(System.currentTimeMillis() - taskStartTimeStampMap.get(taskId)));

                TaskResultDao taskResultDao = new TaskResultDao();

                taskResultDao.insert(task);
                taskResultMap.remove(taskId);
                taskSizeMap.remove(taskId);
                taskResultComparedMap.remove(taskId);
                taskStartTimeStampMap.remove(taskId);
            } else {
                taskResultComparedMap.put(taskId, taskResultCompared);
                logger.info("Compared video pair: " + taskResultCompared);
            }
        }
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
