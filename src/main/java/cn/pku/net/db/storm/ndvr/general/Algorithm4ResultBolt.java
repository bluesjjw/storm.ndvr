/**
 * @Package cn.pku.net.db.storm.ndvr.bolt
 * Created by jeremyjiang on 2016/5/13.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.general;

import java.lang.reflect.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.dao.TaskResultDao;
import cn.pku.net.db.storm.ndvr.entity.GlobalSimilarVideo;
import cn.pku.net.db.storm.ndvr.entity.TaskEntity;

/**
 * Description: Use global visual feature
 *
 * @author jeremyjiang
 * Created at 2016/5/13 9:51
 */
public class Algorithm4ResultBolt extends BaseBasicBolt {

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
     * @see backtype.storm.topology.IBasicBolt#execute(backtype.storm.tuple.Tuple, backtype.storm.topology.BasicOutputCollector) backtype.storm.topology.IBasicBolt#execute(backtype.storm.tuple.Tuple, backtype.storm.topology.BasicOutputCollector)
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String              taskId          = input.getStringByField("taskId");
        String              taskType        = input.getStringByField("taskType");
        int                 fieldGroupingId = input.getIntegerByField("fieldGroupingId");
        Map<String, String> ctrlMsg         = (Map<String, String>) input.getValue(3);    // 控制信息

        // retrieval任务,一个query视频
        if (Const.STORM_CONFIG.RETRIEVAL_TASK_FLAG.equals(taskType)) {

            // query视频
            // String queryVideoInfoStr = ctrlMsg.get("queryVideo");
            // VideoInfoEntity queryVideoInfo = (new Gson()).fromJson(queryVideoInfoStr,
            // VideoInfoEntity.class);
            String                   similarVideoListStr  = ctrlMsg.get("globalSimilarVideoList");
            Type                     similarVideoListType = new TypeToken<List<GlobalSimilarVideo>>() {}
            .getType();
            List<GlobalSimilarVideo> similarVideoList     = (new Gson()).fromJson(similarVideoListStr,
                                                                                  similarVideoListType);
            TaskEntity task = new TaskEntity();

            task.setTaskId(taskId);
            task.setTaskType(taskType);

            List<String> videoIdList = new ArrayList<String>();

            if (null != similarVideoList) {
                for (GlobalSimilarVideo similarVideo : similarVideoList) {
                    videoIdList.add(similarVideo.getVideoId());
                }
            }

            task.setVideoIdList(videoIdList);
            task.setStatus("1");

            long startTimeStamp = Long.parseLong(ctrlMsg.get("startTimeStamp"));

            task.setTimeStamp(Long.toString(System.currentTimeMillis() - startTimeStamp));

            TaskResultDao taskResultDao = new TaskResultDao();

            taskResultDao.insert(task);
        }
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {}
}


//~ Formatted by Jindent --- http://www.jindent.com
