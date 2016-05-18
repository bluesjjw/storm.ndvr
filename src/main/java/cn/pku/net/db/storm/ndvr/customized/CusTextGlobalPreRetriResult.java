/**
 * @Package cn.pku.net.db.storm.ndvr.customized
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.customized;

import java.lang.reflect.Type;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import cn.pku.net.db.storm.ndvr.dao.TaskResultDao;
import cn.pku.net.db.storm.ndvr.entity.GlobalSimilarVideo;
import cn.pku.net.db.storm.ndvr.entity.TaskEntity;

/**
 * Description: Customized result bolt for pre-filtering retrieval task, save the textual and global similar video list to MongoDB
 *
 * @author jeremyjiang
 * Created at 2016/5/12 20:48
 */
public class CusTextGlobalPreRetriResult extends BaseBasicBolt {

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
        String                   taskId               = input.getStringByField("taskId");
        String                   taskType             = input.getStringByField("taskType");
        String                   similarVideoListStr  = input.getStringByField("similarVideoList");
        Type                     similarVideoListType = new TypeToken<List<GlobalSimilarVideo>>() {}
        .getType();
        List<GlobalSimilarVideo> similarVideoList     = (new Gson()).fromJson(similarVideoListStr,
                                                                              similarVideoListType);
        TaskEntity               task                 = new TaskEntity();

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

        long startTimeStamp = input.getLongByField("startTimeStamp");

        task.setTimeStamp(Long.toString(System.currentTimeMillis() - startTimeStamp));

        TaskResultDao taskResultDao = new TaskResultDao();

        taskResultDao.insert(task);
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
