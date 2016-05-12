/**
 * @Title: CustomizedTextDetectionSpout.java 
 * @Package cn.pku.net.db.storm.ndvr.customized.text 
 * @Description: TODO
 * @author Jiawei Jiang    
 * @date 2015年1月30日 下午4:18:43 
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved.
 */
package cn.pku.net.db.storm.ndvr.customized;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

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

import com.google.gson.Gson;

/**
 * @ClassName: CustomizedTextDetectionSpout 
 * @Description: TODO
 * @author Jiawei Jiang
 * @date 2015年1月30日 下午4:18:43
 */
public class CustomizedDetectionSpout implements IRichSpout {

    private SpoutOutputCollector                collector;
    private TopologyContext                     context;

    private boolean                             completed       = false;
    private int                                 interval        = Const.STORM_CONFIG.GET_TASK_INTERVAL;            //查询任务的间隔，单位为毫秒

    private static final Logger                 logger          = Logger
                                                                    .getLogger(CustomizedDetectionSpout.class);

    private static Map<String, VideoInfoEntity> cachedVideoInfo = new ConcurrentHashMap<String, VideoInfoEntity>();

    /** 
     * @see backtype.storm.spout.ISpout#open(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.spout.SpoutOutputCollector)
     */
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.context = context;
        this.collector = collector;
    }

    /** 
     * @see backtype.storm.spout.ISpout#close()
     */
    public void close() {
    }

    /** 
     * @see backtype.storm.spout.ISpout#activate()
     */
    public void activate() {
    }

    /** 
     * @see backtype.storm.spout.ISpout#deactivate()
     */
    public void deactivate() {
    }

    /** 
     * @see backtype.storm.spout.ISpout#nextTuple()
     */
    public void nextTuple() {
        if (!completed) {
            long startTimeStamp = System.currentTimeMillis();
            TaskDao taskDao = new TaskDao();
            List<TaskEntity> taskList = taskDao.getNewDetectionTask();
            if (null == taskList || taskList.isEmpty()) {
                return;
            }
            for (TaskEntity task : taskList) {
                String taskId = task.getTaskId();
                String taskType = task.getTaskType();
                VideoInfoDao videoInfoDao = new VideoInfoDao();
                //取出task对应的query视频id
                List<String> videoIdList = task.getVideoIdList();
                List<VideoInfoEntity> videoInfoList = new ArrayList<VideoInfoEntity>();
                for (String videoId : videoIdList) {
                    if (cachedVideoInfo.containsKey(videoId)) {
                        videoInfoList.add(cachedVideoInfo.get(videoId));
                    } else {
                        VideoInfoEntity videoInfo = videoInfoDao.getVideoInfoById(videoId);
                        if (null == videoInfo) {
                            logger.info("Invalid video: " + videoId);
                            continue;
                        }
                        videoInfoList.add(videoInfo);
                        cachedVideoInfo.put(videoId, videoInfo);
                    }
                }
                for (VideoInfoEntity videoInfo1 : videoInfoList) {
                    for (VideoInfoEntity videoInfo2 : videoInfoList) {
                        //保证第一个视频的id小于第二个视频的id
                        if (Integer.parseInt(videoInfo1.getVideoId()) < Integer.parseInt(videoInfo2
                            .getVideoId())) {
                            Gson gson = new Gson();
                            String queryVideoStr1 = gson.toJson(videoInfo1);
                            String queryVideoStr2 = gson.toJson(videoInfo2);
                            collector.emit(new Values(taskId, taskType, queryVideoStr1,
                                queryVideoStr2, startTimeStamp));
                        }
                    }
                }
            }
            //            try {
            //                Thread.sleep(interval);
            //            } catch (InterruptedException e) {
            //                logger.error("InterruptedException during thread sleep", e);
            //            }
        }

    }

    /** 
     * @see backtype.storm.spout.ISpout#ack(java.lang.Object)
     */
    public void ack(Object msgId) {
        logger.info("OK: " + msgId);
    }

    /** 
     * @see backtype.storm.spout.ISpout#fail(java.lang.Object)
     */
    public void fail(Object msgId) {
        logger.info("FAIL: " + msgId);
    }

    /** 
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("taskId", "taskType", "queryVideo1", "queryVideo2",
            "startTimeStamp"));
    }

    /** 
     * @see backtype.storm.topology.IComponent#getComponentConfiguration()
     */
    public Map<String, Object> getComponentConfiguration() {
        return null;
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
