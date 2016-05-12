/**
 * @Title: CustomizedTextRetrievalResult.java 
 * @Package cn.pku.net.db.storm.ndvr.customized.text 
 * @Description: TODO
 * @author Jiawei Jiang    
 * @date 2015年1月28日 下午9:10:54 
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved.
 */
package cn.pku.net.db.storm.ndvr.customized;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import cn.pku.net.db.storm.ndvr.dao.TaskResultDao;
import cn.pku.net.db.storm.ndvr.entity.TaskEntity;
import cn.pku.net.db.storm.ndvr.entity.TextSimilarVideo;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * @ClassName: CustomizedTextRetrievalResult
 * @Description: TODO
 * @author Jiawei Jiang
 * @date 2015年1月28日 下午9:10:54
 */
public class CustomizedTextRetrievalResult extends BaseBasicBolt {

    /** 
     * @see backtype.storm.topology.IBasicBolt#execute(backtype.storm.tuple.Tuple, backtype.storm.topology.BasicOutputCollector)
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String taskId = input.getStringByField("taskId");
        String taskType = input.getStringByField("taskType");
        String similarVideoListStr = input.getStringByField("textSimilarVideoList");
        Type similarVideoListType = new TypeToken<List<TextSimilarVideo>>() {
        }.getType();
        List<TextSimilarVideo> similarVideoList = (new Gson()).fromJson(similarVideoListStr,
            similarVideoListType);
        TaskEntity task = new TaskEntity();
        task.setTaskId(taskId);
        task.setTaskType(taskType);
        List<String> videoIdList = new ArrayList<String>();
        for (TextSimilarVideo similarVideo : similarVideoList) {
            videoIdList.add(similarVideo.getVideoId());
        }
        task.setVideoIdList(videoIdList);
        task.setStatus("1");
        long startTimeStamp = input.getLongByField("startTimeStamp");
        task.setTimeStamp(Long.toString(System.currentTimeMillis() - startTimeStamp));
        TaskResultDao taskResultDao = new TaskResultDao();
        taskResultDao.insert(task);
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
