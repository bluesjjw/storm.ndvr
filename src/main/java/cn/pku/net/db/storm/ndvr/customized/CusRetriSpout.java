/**
 * @Package cn.pku.net.db.storm.ndvr.customized
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.customized;

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
 * Description: Customized spout for retrieval task
 *
 * @author jeremyjiang
 * Created at 2016/5/12 20:36
 */
public class CusRetriSpout implements IRichSpout {
    private static final Logger  logger    = Logger.getLogger(CusRetriSpout.class);
    private boolean              completed = false;
    private int                  interval  = Const.STORM_CONFIG.GET_TASK_INTERVAL;    // 查询任务的间隔，单位为秒
    private SpoutOutputCollector collector;
    private TopologyContext      context;

    /**
     * Ack.
     *
     * @param msgId the msg id
     * @see backtype.storm.spout.ISpout#ack(java.lang.Object) backtype.storm.spout.ISpout#ack(java.lang.Object)
     */
    public void ack(Object msgId) {
        logger.info("OK: " + msgId);
    }

    /**
     * Activate.
     *
     * @see backtype.storm.spout.ISpout#activate() backtype.storm.spout.ISpout#activate()
     */
    public void activate() {}

    /**
     * Close.
     *
     * @see backtype.storm.spout.ISpout#close() backtype.storm.spout.ISpout#close()
     */
    public void close() {}

    /**
     * Deactivate.
     *
     * @see backtype.storm.spout.ISpout#deactivate() backtype.storm.spout.ISpout#deactivate()
     */
    public void deactivate() {}

    /**
     * Declare output fields.
     *
     * @param declarer the declarer
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer) backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("taskId", "taskType", "queryVideo", "startTimeStamp", "fieldGroupingId"));
    }

    /**
     * Fail.
     *
     * @param msgId the msg id
     * @see backtype.storm.spout.ISpout#fail(java.lang.Object) backtype.storm.spout.ISpout#fail(java.lang.Object)
     */
    public void fail(Object msgId) {
        logger.info("FAIL: " + msgId);
    }

    /**
     * Next tuple.
     *
     * @see backtype.storm.spout.ISpout#nextTuple() backtype.storm.spout.ISpout#nextTuple()
     */
    public void nextTuple() {
        if (!completed) {
            long             startTimeStamp = System.currentTimeMillis();
            TaskDao          taskDao        = new TaskDao();
            List<TaskEntity> taskList       = taskDao.getNewRetrievalTask();

            if ((null == taskList) || taskList.isEmpty()) {
                return;
            }

            for (TaskEntity task : taskList) {
                String       taskId       = task.getTaskId();
                String       taskType     = task.getTaskType();
                VideoInfoDao videoInfoDao = new VideoInfoDao();
                // 取出task对应的query视频id
                List<String>    videoIdList     = task.getVideoIdList();
                VideoInfoEntity queryVideo      = videoInfoDao.getVideoInfoById(videoIdList.get(0));
                Gson            gson            = new Gson();
                String          queryVideoStr   = gson.toJson(queryVideo);
                int             fieldGroupingId = queryVideo.getDuration() / Const.STORM_CONFIG.BOLT_DURATION_WINDOW;
                this.collector.emit(new Values(taskId, taskType, queryVideoStr, startTimeStamp, fieldGroupingId));
            }

            try {
                Thread.sleep(interval * 1000);
            } catch (InterruptedException e) {
                logger.error("InterruptedException during thread sleep", e);
            }
        }
    }

    /**
     * Open.
     *
     * @param conf      the conf
     * @param context   the context
     * @param collector the collector
     * @see backtype.storm.spout.ISpout#open(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.spout.SpoutOutputCollector) backtype.storm.spout.ISpout#open(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.spout.SpoutOutputCollector)
     */
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.context   = context;
        this.collector = collector;
    }

    /**
     * Gets component configuration.
     *
     * @return the component configuration
     * @see backtype.storm.topology.IComponent#getComponentConfiguration() backtype.storm.topology.IComponent#getComponentConfiguration()
     */
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
