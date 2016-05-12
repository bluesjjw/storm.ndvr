/**
 * @Package cn.pku.net.db.storm.ndvr.dao
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.dao;

import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import com.google.gson.Gson;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.entity.HSVSigEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoHSVSigEntity;

/**
 * Description: Data access object for HSV signature
 *
 * @author jeremyjiang
 * Created at 2016/5/12 18:51
 */
public class HSVSignatureDao {
    private static final Logger logger      = Logger.getLogger(HSVSignatureDao.class);
    private static MongoClient  mongoClient = null;

    /**
     * Instantiates
     */
    public HSVSignatureDao() {
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
        HSVSignatureDao   dao = new HSVSignatureDao();
        VideoHSVSigEntity ent = dao.getVideoHSVSigById("1");

        System.out.println(ent.getSig());
    }

    /**
     * search by videoid.
     *
     * @param videoId the video id
     * @return the video HSV signature
     */
    public VideoHSVSigEntity getVideoHSVSigById(String videoId) {
        VideoHSVSigEntity ent = null;

        if (null == mongoClient) {
            return null;
        }

        DB            db          = mongoClient.getDB(Const.MONGO.MONGO_DATABASE);
        DBCollection  col         = db.getCollection(Const.MONGO.MONGO_HSVSIG_COLLECTION);
        BasicDBObject query       = new BasicDBObject("videoId", videoId);
        DBObject      queryResult = col.findOne(query);

        if (null != queryResult) {
            String       sigStr = queryResult.get("signature").toString();
            HSVSigEntity sigEnt = (new Gson()).fromJson(sigStr, HSVSigEntity.class);

            ent = new VideoHSVSigEntity(videoId, sigEnt);
        }

        return ent;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
