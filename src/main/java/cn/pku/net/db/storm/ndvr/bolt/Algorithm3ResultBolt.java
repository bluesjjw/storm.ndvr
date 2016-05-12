/**
 * @Title: Algorithm3ResultBolt.java 
 * @Package cn.pku.net.db.storm.ndvr.bolt 
 * @Description: TODO
 * @author Jiawei Jiang    
 * @date 2015年1月26日 下午6:59:49 
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved.
 */
package cn.pku.net.db.storm.ndvr.bolt;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.dao.TaskResultDao;
import cn.pku.net.db.storm.ndvr.entity.GlobalSimilarVideo;
import cn.pku.net.db.storm.ndvr.entity.TaskEntity;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * @ClassName: Algorithm3ResultBolt
 * @Description: 使用文本信息和全局特征
 * @author Jiawei Jiang
 * @date 2015年1月26日 下午6:59:49
 */
public class Algorithm3ResultBolt extends BaseBasicBolt {

    /** 
     * @see backtype.storm.topology.IBasicBolt#execute(backtype.storm.tuple.Tuple, backtype.storm.topology.BasicOutputCollector)
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String taskId = input.getStringByField("taskId");
        String taskType = input.getStringByField("taskType");
        int fieldGroupingId = input.getIntegerByField("fieldGroupingId");
    Map<String, String> ctrlMsg = (Map<String, String>) input.getValue(3); // 控制信息
    // retrieval任务,一个query视频
        if (Const.STORM_CONFIG.RETRIEVAL_TASK_FLAG.equals(taskType)) {
      // query视频
            //            String queryVideoInfoStr = ctrlMsg.get("queryVideo");
            //            VideoInfoEntity queryVideoInfo = (new Gson()).fromJson(queryVideoInfoStr,
            //                VideoInfoEntity.class);
            String similarVideoListStr = ctrlMsg.get("globalSimilarVideoList");
            Type similarVideoListType = new TypeToken<List<GlobalSimilarVideo>>() {
            }.getType();
            List<GlobalSimilarVideo> similarVideoList = (new Gson()).fromJson(similarVideoListStr,
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
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
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
