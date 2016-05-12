/**
 * @Package cn.pku.net.db.storm.ndvr.dao
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.dao;

import java.lang.reflect.Type;

import java.net.UnknownHostException;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.entity.TaskEntity;

/**
 * Description: Data access object for result
 *
 * @author jeremyjiang
 * Created at 2016/5/12 19:50
 */
public class TaskResultDao {
    private static final Logger logger      = Logger.getLogger(TaskResultDao.class);
    private static MongoClient  mongoClient = null;

    /**
     * Instantiates
     */
    public TaskResultDao() {
        if (null == mongoClient) {
            try {
                mongoClient = new MongoClient(Const.MONGO.MONGO_TASK_HOST, Const.MONGO.MONGO_PORT);
            } catch (UnknownHostException e) {
                logger.error("MongoDB UnknownHost", e);
            }
        }
    }

    /**
     * Insert task result
     *
     * @param task the task
     */
    public void insert(TaskEntity task) {
        if (null == task) {
            return;
        }

        DB           db      = mongoClient.getDB(Const.MONGO.MONGO_DATABASE);
        DBCollection col     = db.getCollection(Const.MONGO.MONGO_TASK_RESULT_COLLECTION);
        String       jsonStr = (new Gson()).toJson(task);
        DBObject     obj     = (DBObject) JSON.parse(jsonStr);

        col.insert(obj);
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {
        TaskResultDao    dao    = new TaskResultDao();
        List<TaskEntity> result = dao.getTaskResult();

        System.out.println(result.size());

        for (TaskEntity ent : result) {

            // System.out.println(ent.getTimeStamp());
        }
    }

    /**
     * Gets task results
     *
     * @return the task result list
     */
    public List<TaskEntity> getTaskResult() {
        if (null == mongoClient) {
            return null;
        }

        List<TaskEntity> result = new ArrayList<TaskEntity>();
        DB               db     = mongoClient.getDB(Const.MONGO.MONGO_DATABASE);
        DBCollection     col    = db.getCollection(Const.MONGO.MONGO_TASK_RESULT_COLLECTION);
        DBCursor         cursor = col.find();

        while (cursor.hasNext()) {
            DBObject obj = cursor.next();

            System.out.println(obj.toString());

            TaskEntity ent = new TaskEntity();

            ent.setTaskId(obj.get("taskId").toString());
            ent.setTaskType((String) obj.get("taskType"));
            ent.setTimeStamp((String) obj.get("timeStamp"));

            String       videoIdListStr  = obj.get("videoIdList").toString();
            Type         videoIdListType = new TypeToken<List<String>>() {}
            .getType();
            List<String> videoIdList     = (new Gson()).fromJson(videoIdListStr, videoIdListType);

            ent.setVideoIdList(videoIdList);
            ent.setStatus(obj.get("status").toString());
            result.add(ent);
        }

        cursor.close();

        return result;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
