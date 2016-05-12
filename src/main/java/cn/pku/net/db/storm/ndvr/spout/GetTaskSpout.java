/**
 * @Package cn.pku.net.db.storm.ndvr.spout
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.spout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.gson.Gson;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.dao.TaskDao;
import cn.pku.net.db.storm.ndvr.dao.VideoInfoDao;
import cn.pku.net.db.storm.ndvr.entity.TaskEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoInfoEntity;

/**
 * Description: Spout gettting tasks
 * @author jeremyjiang
 * Created at 2016/5/12 18:33
 */
public class GetTaskSpout implements IRichSpout {
    private static final Logger  logger    = Logger.getLogger(GetTaskSpout.class);
    private boolean              completed = false;
    private int                  interval  = Const.STORM_CONFIG.GET_TASK_INTERVAL;    // 查询任务的间隔，单位为秒
    private SpoutOutputCollector collector;
    private TopologyContext      context;

    /**
     * @see backtype.storm.spout.ISpout#ack(java.lang.Object)
     */
    public void ack(Object msgId) {
        logger.info("OK: " + msgId);
    }

    /**
     * @see backtype.storm.spout.ISpout#activate()
     */
    public void activate() {}

    /**
     * @see backtype.storm.spout.ISpout#close()
     */
    public void close() {}

    /**
     * @see backtype.storm.spout.ISpout#deactivate()
     */
    public void deactivate() {}

    /**
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("taskId", "taskType", "fieldGroupingId", "ctrlMsg"));
    }

    /**
     * @see backtype.storm.spout.ISpout#fail(java.lang.Object)
     */
    public void fail(Object msgId) {
        logger.info("FAIL: " + msgId);
    }

    /**
     * @see backtype.storm.spout.ISpout#nextTuple()
     */
    public void nextTuple() {
        if (!completed) {
            TaskDao          taskDao  = new TaskDao();
            List<TaskEntity> taskList = taskDao.getNewTask();

            if ((null == taskList) || taskList.isEmpty()) {
                return;
            }

            for (TaskEntity task : taskList) {
                String       taskId       = task.getTaskId();
                String       taskType     = task.getTaskType();
                VideoInfoDao videoInfoDao = new VideoInfoDao();

                // 取出task对应的query视频id
                List<String>          videoIdList   = task.getVideoIdList();
                List<VideoInfoEntity> videoInfoList = new ArrayList<VideoInfoEntity>();

                for (String videoId : videoIdList) {
                    videoInfoList.add(videoInfoDao.getVideoInfoById(videoId));
                }

                Map<String, String> ctrlMsg = new HashMap<String, String>();    // 控制信息,key为String,value为Gson生成的JSON字符串

                // 如果是retrieval任务
                if (Const.STORM_CONFIG.RETRIEVAL_TASK_FLAG.equals(taskType)) {
                    Gson   gson    = new Gson();
                    String gsonStr = gson.toJson(videoInfoList.get(0));

                    ctrlMsg.put("startTimeStamp", Long.toString(System.currentTimeMillis()));    // 记录任务开始时的时间
                    ctrlMsg.put("queryVideo", gsonStr);

                    // 以duration做fieldGrouping,一个bolt负责的时长范围为Const.STORM_CONFIG.BOLT_DURATION_WINDOW
                    int fieldGroupingId = videoInfoList.get(0).getDuration() / Const.STORM_CONFIG.BOLT_DURATION_WINDOW;

                    this.collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
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

                                // 控制fieldGrouping,一个bolt负责一个时间段的视频,detection任务其实不需要fieldGrouping
                                int fieldGroupingId = ent1.getDuration() / Const.STORM_CONFIG.BOLT_DURATION_WINDOW;

                                collector.emit(new Values(taskId, taskType, fieldGroupingId, ctrlMsg));
                            }
                        }
                    }
                }
            }

            try {
                Thread.sleep(interval * 1000);
            } catch (InterruptedException e) {
                logger.error("InterruptedException during thread sleep", e);
            }
        }
    }

    /**
     * @see backtype.storm.spout.ISpout#open(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.spout.SpoutOutputCollector)
     */
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.context   = context;
        this.collector = collector;
    }

    /**
     * @see backtype.storm.topology.IComponent#getComponentConfiguration()
     */
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
