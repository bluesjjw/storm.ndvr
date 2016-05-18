/**
 * Created by jeremyjiang on 2016/5/18.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */
package cn.pku.net.db.storm.ndvr.customized;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.dao.TaskDao;
import cn.pku.net.db.storm.ndvr.dao.TaskResultDao;
import cn.pku.net.db.storm.ndvr.entity.TaskEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoInfoEntity;
import com.google.gson.Gson;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Description:
 *
 * @author jeremyjiang
 *         Created at 2016/5/18 10:15
 */

public class CusTextGlobalPostDetecResult extends BaseBasicBolt {
    private static final Logger logger                = Logger.getLogger(CusTextGlobalPostDetecResult.class);
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
    public void declareOutputFields(OutputFieldsDeclarer declarer) { }

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
        String          queryVideoStr1 = input.getStringByField("queryVideo1");
        String          queryVideoStr2 = input.getStringByField("queryVideo2");
        long            startTimeStamp = input.getLongByField("startTimeStamp");
        String similarType = input.getFields().get(4);

        // texual similarity
        if (similarType.equals("textSimilarity")) {
            if (!taskResultMap.containsKey(taskId)) {
                TaskEntity task = (new TaskDao()).getTaskById(taskId);
                taskResultMap.put(taskId, task.getVideoIdList());
                taskSizeMap.put(taskId, task.getVideoIdList().size());
                taskResultComparedMap.put(taskId, 0);
                taskStartTimeStampMap.put(taskId, startTimeStamp);
            }

            float textSimilarity = input.getFloatByField("textSimilarity");

            if (textSimilarity >= Const.STORM_CONFIG.TEXT_SIMILARITY_THRESHOLD) {
                VideoInfoEntity queryVideo1 = (new Gson()).fromJson(queryVideoStr1, VideoInfoEntity.class);
                VideoInfoEntity queryVideo2 = (new Gson()).fromJson(queryVideoStr2, VideoInfoEntity.class);
                List<String> videoIdList = taskResultMap.get(taskId);
                int index1 = videoIdList.indexOf(queryVideo1.getVideoId());
                int index2 = videoIdList.indexOf(queryVideo2.getVideoId());
                if ((index1 == -1) || (index2 == -1)) { // remove the latter video in the result video list
                } else if (index1 > index2) {
                    videoIdList.remove(index1);
                    logger.info(String.format("TaskId: %s, remove video: %s w.r.t. global visual similarity",
                            taskId, index1));
                } else {
                    videoIdList.remove(index2);
                    logger.info(String.format("TaskId: %s, remove video: %s w.r.t. global visual similarity",
                            taskId, index2));
                }
                taskResultMap.put(taskId, videoIdList);
            }
        }
        // global visual signature distance
        else if (similarType.equals("globalDistance")){
            if (!taskResultMap.containsKey(taskId)) {
                TaskEntity task = (new TaskDao()).getTaskById(taskId);
                taskResultMap.put(taskId, task.getVideoIdList());
                taskSizeMap.put(taskId, task.getVideoIdList().size());
                taskResultComparedMap.put(taskId, 0);
                taskStartTimeStampMap.put(taskId, startTimeStamp);
            }

            float globalDistance = input.getFloatByField("globalDistance");
            if (globalDistance > Const.STORM_CONFIG.GLOBALSIG_EUCLIDEAN_THRESHOLD) {
                VideoInfoEntity queryVideo1 = (new Gson()).fromJson(queryVideoStr1, VideoInfoEntity.class);
                VideoInfoEntity queryVideo2 = (new Gson()).fromJson(queryVideoStr2, VideoInfoEntity.class);
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