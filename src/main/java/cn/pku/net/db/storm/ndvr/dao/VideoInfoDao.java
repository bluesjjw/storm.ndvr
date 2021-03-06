/**
 * @Package cn.pku.net.db.storm.ndvr.dao
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import java.net.UnknownHostException;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.gson.Gson;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.entity.VideoInfoEntity;

/**
 * Description: Data access object for video info
 *
 * @author jeremyjiang
 * Created at 2016/5/12 19:52
 */
public class VideoInfoDao {
    private static final Logger logger      = Logger.getLogger(VideoInfoDao.class);
    private static MongoClient  mongoClient = null;

    /**
     * Instantiates a new Video info dao.
     */
    public VideoInfoDao() {
        if (null == mongoClient) {
            try {
                mongoClient = new MongoClient(Const.MONGO.MONGO_HOST, Const.MONGO.MONGO_PORT);
            } catch (UnknownHostException e) {
                logger.error("MongoDB UnknownHost", e);
            }
        }
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {
        VideoInfoDao dao = new VideoInfoDao();

        // dao.saveToMongo();
        VideoInfoEntity ent = dao.getVideoInfoById("773");

        System.out.println(ent.getTitle());

        // List<VideoInfoEntity> entList = dao.getVideoInfoByDuration(55, 58);
        // Collections.sort(entList, new VideoInfoEntity());
        // System.out.println(entList.size());
        // for (VideoInfoEntity ent : entList) {
        // System.out.println(ent.getDuration());
        // }

        // int count = 0;
        // File file = new File(Const.CC_WEB_VIDEO.VIDEO_INFO_PATH);
        // BufferedReader reader = null;
        // try {
        // InputStreamReader input = new InputStreamReader(new FileInputStream(file), "GBK");
        // reader = new BufferedReader(input);
        // String line = null;
        // reader.readLine();
        // while ((line = reader.readLine()) != null) {
        // count++;
        // String[] infos = line.split("\\t");
        // if (infos.length >= 10 && count == 9893) {
        ////                    line = new String(line.getBytes("GBK"), "UTF-8");
        // System.out.println(line);
        // System.out.println(infos[6]);
        ////                    String title = new String(infos[6].getBytes("GB2312"), "UTF-8");
        ////                    System.out.println(title);
        ////                    title = new String(title.getBytes("GB2312"), "UTF-8");
        ////                    System.out.println(title);
        // }
        // }
        // reader.close();
        // } catch (FileNotFoundException e) {
        // logger.error("", e);
        // } catch (IOException e) {
        // logger.error("", e);
        // }
    }

    /**
     * Save all the video infos in Video_Complete.txt to MongoDB
     */
    public void saveToMongo() {
        try {
            DB           mongoDB    = mongoClient.getDB(Const.MONGO.MONGO_DATABASE);
            DBCollection collection = mongoDB.getCollection(Const.MONGO.MONGO_VIDEO_COLLECTION);
            File         file       = new File(Const.CC_WEB_VIDEO.VIDEO_INFO_PATH);

            // txt文件编码方式为gbk,否则会出现乱码
            InputStreamReader input  = new InputStreamReader(new FileInputStream(file), "gbk");
            BufferedReader    reader = null;

            reader = new BufferedReader(input);

            String line = null;

            reader.readLine();

            while ((line = reader.readLine()) != null) {
                String[] infos = line.split("\\t");

                if (infos.length >= 10) {
                    VideoInfoEntity ent = VideoInfoEntity.parse(infos);

                    if (null != ent) {
                        Gson   gson    = new Gson();
                        String gsonStr = gson.toJson(ent);

                        // logger.info(gsonStr);
                        DBObject obj = (DBObject) JSON.parse(gsonStr);

                        collection.insert(obj);
                    }
                }
            }

            reader.close();
        } catch (UnknownHostException e) {
            logger.error("Unknown MongoDB host. ", e);
        } catch (FileNotFoundException e) {
            logger.error("File of HSV Signature is not found. ", e);
        } catch (IOException e) {
            logger.error("IO error. ", e);
        }
    }

    /**
     * Gets all video infos.
     *
     * @return all video infos
     */
    public List<VideoInfoEntity> getAllVideoInfo() {
        List<VideoInfoEntity> entList = new ArrayList<VideoInfoEntity>();

        if (null == mongoClient) {
            return null;
        }

        DB           db     = mongoClient.getDB(Const.MONGO.MONGO_DATABASE);
        DBCollection col    = db.getCollection(Const.MONGO.MONGO_VIDEO_COLLECTION);
        DBCursor     cursor = col.find();

        while (cursor.hasNext()) {
            VideoInfoEntity ent = new VideoInfoEntity();
            DBObject        obj = cursor.next();

            ent.setVideoId((String) obj.get("videoId"));
            ent.setTopicId((String) obj.get("topicId"));
            ent.setSource((String) obj.get("source"));
            ent.setVideoFileName((String) obj.get("videoFileName"));
            ent.setDuration((Integer) obj.get("duration"));
            ent.setFormat((String) obj.get("format"));
            ent.setTitle((String) obj.get("title"));
            ent.setUrl((String) obj.get("url"));
            entList.add(ent);
        }

        return entList;
    }

    /**
     * Gets videos by duration
     * 查询videoInfo表时长为duration的视频(注:数据集中有些视频时长数据没有,我们设为0)
     *
     * @param duration the duration
     * @return the video infos
     */
    public List<VideoInfoEntity> getVideoInfoByDuration(int duration) {
        List<VideoInfoEntity> entList = new ArrayList<VideoInfoEntity>();

        if (null == mongoClient) {
            return null;
        }

        DB            db     = mongoClient.getDB(Const.MONGO.MONGO_DATABASE);
        DBCollection  col    = db.getCollection(Const.MONGO.MONGO_VIDEO_COLLECTION);
        BasicDBObject query  = new BasicDBObject("duration", duration);
        DBCursor      cursor = col.find(query);

        while (cursor.hasNext()) {
            VideoInfoEntity ent = new VideoInfoEntity();
            DBObject        obj = cursor.next();

            ent.setVideoId((String) obj.get("videoId"));
            ent.setTopicId((String) obj.get("topicId"));
            ent.setSource((String) obj.get("source"));
            ent.setVideoFileName((String) obj.get("videoFileName"));
            ent.setDuration((Integer) obj.get("duration"));
            ent.setFormat((String) obj.get("format"));
            ent.setTitle((String) obj.get("title"));
            ent.setUrl((String) obj.get("url"));
            entList.add(ent);
        }

        return entList;
    }

    /**
     * Gets videos in a duration span
     * 查询videoInfo表一段时长范围内的视频
     *
     * @param minDuration the min duration
     * @param maxDuration the max duration
     * @return the video infos
     */
    public List<VideoInfoEntity> getVideoInfoByDuration(int minDuration, int maxDuration) {
        if (minDuration <= 0) {
            minDuration = 1;
        }

        List<VideoInfoEntity> entList = new ArrayList<VideoInfoEntity>();

        if (null == mongoClient) {
            return null;
        }

        DB            db    = mongoClient.getDB(Const.MONGO.MONGO_DATABASE);
        DBCollection  col   = db.getCollection(Const.MONGO.MONGO_VIDEO_COLLECTION);
        BasicDBObject query = new BasicDBObject("duration",
                                                new BasicDBObject("$gte", minDuration).append("$lte", maxDuration));
        DBCursor cursor = col.find(query);

        while (cursor.hasNext()) {
            VideoInfoEntity ent = new VideoInfoEntity();
            DBObject        obj = cursor.next();

            ent.setVideoId((String) obj.get("videoId"));
            ent.setTopicId((String) obj.get("topicId"));
            ent.setSource((String) obj.get("source"));
            ent.setVideoFileName((String) obj.get("videoFileName"));
            ent.setDuration((Integer) obj.get("duration"));
            ent.setFormat((String) obj.get("format"));
            ent.setTitle((String) obj.get("title"));
            ent.setUrl((String) obj.get("url"));
            entList.add(ent);
        }

        return entList;
    }

    /**
     * Gets video info by videoid.
     *
     * @param videoId the video id
     * @return the videoinfo
     */
    public VideoInfoEntity getVideoInfoById(String videoId) {
        VideoInfoEntity ent = null;

        if (null == mongoClient) {
            return null;
        }

        DB            db          = mongoClient.getDB(Const.MONGO.MONGO_DATABASE);
        DBCollection  col         = db.getCollection(Const.MONGO.MONGO_VIDEO_COLLECTION);
        BasicDBObject query       = new BasicDBObject("videoId", videoId);
        DBObject      queryResult = col.findOne(query);

        if (null != queryResult) {
            ent = new VideoInfoEntity();
            ent.setVideoId((String) queryResult.get("videoId"));
            ent.setTopicId((String) queryResult.get("topicId"));
            ent.setSource((String) queryResult.get("source"));
            ent.setVideoFileName((String) queryResult.get("videoFileName"));
            ent.setDuration((Integer) queryResult.get("duration"));
            ent.setFormat((String) queryResult.get("format"));
            ent.setTitle((String) queryResult.get("title"));
            ent.setUrl((String) queryResult.get("url"));
        }

        return ent;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
