/**
 * @Package cn.pku.net.db.storm.ndvr.dao
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.dao;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

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
import cn.pku.net.db.storm.ndvr.entity.KeyFrameEntity;

/**
 * Description: Data access object for keyframe
 *
 * @author jeremyjiang  Created at 2016/5/12 18:53
 */
public class KeyFrameDao {
    private static final Logger logger      = Logger.getLogger(KeyFrameDao.class);
    private static MongoClient  mongoClient = null;

    /**
     * Instantiates
     */
    public KeyFrameDao() {
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
        KeyFrameDao dao = new KeyFrameDao();

        dao.saveToMongo();

        // List<KeyFrameEntity> list = dao.getKeyFrameByVideoId("773");
        // for (KeyFrameEntity ent : list) {
        // System.out.println(ent.getKeyFrameName());
        // }
    }

    /**
     * Save keyframe infos in Shot_Info.txt to MongoDB
     */
    public void saveToMongo() {
        try {
            DB             mongoDB    = mongoClient.getDB(Const.MONGO.MONGO_DATABASE);
            DBCollection   collection = mongoDB.getCollection(Const.MONGO.MONGO_KEYFRAME_COLLECTION);
            File           file       = new File(Const.CC_WEB_VIDEO.KEYFRAME_INFO_PATH);
            BufferedReader reader     = null;

            reader = new BufferedReader(new FileReader(file));

            String line = null;

            while ((line = reader.readLine()) != null) {
                String[]       infos = line.split("\\t");
                KeyFrameEntity ent   = KeyFrameEntity.parse(infos);

                if (null != ent) {
                    Gson   gson    = new Gson();
                    String gsonStr = gson.toJson(ent);

                    logger.info(gsonStr);

                    DBObject obj = (DBObject) JSON.parse(gsonStr);

                    collection.insert(obj);
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
     * search keyframe by videoid
     *
     * @param vid the vid
     * @return the key frame by video id
     */
    public List<KeyFrameEntity> getKeyFrameByVideoId(String vid) {
        List<KeyFrameEntity> result = new ArrayList<KeyFrameEntity>();

        if (null == mongoClient) {
            return null;
        }

        DB            db     = mongoClient.getDB(Const.MONGO.MONGO_DATABASE);
        DBCollection  col    = db.getCollection(Const.MONGO.MONGO_KEYFRAME_COLLECTION);
        BasicDBObject query  = new BasicDBObject("videoId", vid);
        DBCursor      cursor = col.find(query);

        while (cursor.hasNext()) {
            DBObject       obj = cursor.next();
            KeyFrameEntity ent = new KeyFrameEntity();

            ent.setVideoId(obj.get("videoId").toString());
            ent.setKeyFrameName(obj.get("keyFrameName").toString());
            ent.setVideoFileName(obj.get("videoFileName").toString());
            ent.setSerialId(obj.get("serialId").toString());
            result.add(ent);
        }

        cursor.close();

        return result;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
