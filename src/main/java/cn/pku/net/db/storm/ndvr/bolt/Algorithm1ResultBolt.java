/**
 * @Title: Algorithm1ResultBolt.java 
 * @Package cn.pku.net.db.storm.ndvr.bolt 
 * @Description: TODO
 * @author Jiawei Jiang    
 * @date 2015年1月12日 上午10:59:59 
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved.
 */
package cn.pku.net.db.storm.ndvr.bolt;

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
import cn.pku.net.db.storm.ndvr.entity.LocalSimilarVideo;
import cn.pku.net.db.storm.ndvr.entity.TaskEntity;

/**
 * @ClassName: Algorithm1ResultBolt
 * @Description: 使用全局特征和局部特征的多模态算法,filter-and-refine策略
 * @author Jiawei Jiang
 * @date 2015年1月12日 上午10:59:59
 */
public class Algorithm1ResultBolt extends BaseBasicBolt {

  /**
   * @see backtype.storm.topology.IBasicBolt#execute(backtype.storm.tuple.Tuple,
   *      backtype.storm.topology.BasicOutputCollector)
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
      // String queryVideoInfoStr = ctrlMsg.get("queryVideo");
      // VideoInfoEntity queryVideoInfo = (new
      // Gson()).fromJson(queryVideoInfoStr,
      // VideoInfoEntity.class);
      String similarVideoListStr = ctrlMsg.get("localSimilarVideoList");
      Type similarVideoListType = new TypeToken<List<LocalSimilarVideo>>() {
      }.getType();
      List<LocalSimilarVideo> similarVideoList = (new Gson())
          .fromJson(similarVideoListStr, similarVideoListType);
      TaskEntity task = new TaskEntity();
      task.setTaskId(taskId);
      task.setTaskType(taskType);
      List<String> videoIdList = new ArrayList<String>();
      if (null != similarVideoList) {
        for (LocalSimilarVideo similarVideo : similarVideoList) {
          videoIdList.add(similarVideo.getVideoId());
        }
      }
      task.setVideoIdList(videoIdList);
      task.setStatus("1");
      long startTimeStamp = Long.parseLong(ctrlMsg.get("startTimeStamp"));
      task.setTimeStamp(
          Long.toString(System.currentTimeMillis() - startTimeStamp));
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
   * @param @param
   *          args
   * @return void
   * @throws @param
   *           args
   */
  public static void main(String[] args) {

  }

}
