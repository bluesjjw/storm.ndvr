/**
 * @Package cn.pku.net.db.storm.ndvr.util
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.util;

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

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.dao.KeyFrameDao;
import cn.pku.net.db.storm.ndvr.dao.VideoInfoDao;
import cn.pku.net.db.storm.ndvr.entity.HSVSigEntity;
import cn.pku.net.db.storm.ndvr.entity.KeyFrameEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoHSVSigEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoInfoEntity;

/**
 * Description: Store HSV Signature to MongoDB
 *
 * @author jeremyjiang  Created at 2016/5/12 17:07
 */
public class HSVSig2Mongo {
    private static final Logger logger = Logger.getLogger(HSVSig2Mongo.class);
    private final String        host;
    private final int           port;
    private final String        dbName;
    private final String        colName;

    /**
     * Instantiates a new HsvSig2Mongo instance.
     *
     * @param host    the host
     * @param port    the port
     * @param dbName  the db name
     * @param colName the col name
     */
    public HSVSig2Mongo(String host, int port, String dbName, String colName) {
        this.host    = host;
        this.port    = port;
        this.dbName  = dbName;
        this.colName = colName;
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {
        HSVSig2Mongo instance = new HSVSig2Mongo(Const.MONGO.MONGO_HOST,
                                                 Const.MONGO.MONGO_PORT,
                                                 Const.MONGO.MONGO_DATABASE,
                                                 Const.MONGO.MONGO_HSVSIG_COLLECTION);

        instance.saveFromFile();

        // instance.save();
        int[] invalid = {
            8999, 9100, 8565, 8714, 8742, 8798, 8835, 8920, 12560, 12582, 12823, 12870, 12880, 12885, 12910, 12927,
            12929, 12964, 12966, 12967, 12968, 12969, 12970, 12971, 12972, 12973, 12976, 12978, 12979, 12981, 12983,
            12986, 13019, 1385, 1387, 3921, 3977, 4003, 4039, 4231, 4251, 4255, 4257, 4270, 4272, 4292, 4491, 4550,
            4616, 4824, 4922, 4956, 4991, 5052, 5361, 5403, 5486, 5494, 5521, 5544, 5547, 5668, 5709, 5738, 5774, 5808,
            5825, 5868, 5894, 5929, 5985, 6487, 6499, 6501, 6511, 6536, 6578, 6623, 6727, 6795, 6913, 6915, 7037, 7077,
            7108, 7152, 7451, 7471, 7490, 7647, 7870, 7896, 7976, 8035, 8063, 8081, 8292, 8324, 8353, 8376, 8384, 8467,
            8626, 8750, 9139, 9313, 9861, 10425, 10496, 10519, 10521, 10581, 10633, 10636, 10708, 10720, 10813, 10940,
            10950, 11084, 11196, 11235, 11335, 11541, 11629, 11759, 11791, 11872, 11943, 12002, 12080, 12115, 12142,
            12155, 12181, 12264, 12379, 12404, 12407, 12483, 33, 53, 87, 129, 150, 229, 271, 344, 345, 362, 380, 414,
            442, 456, 488, 499, 548, 618, 625, 760, 835, 873, 894, 1054, 1121, 1133, 1139, 1287, 1372, 1380, 1382, 1549,
            1626, 1654, 1785, 1863, 1920, 1937, 1955, 1988, 2003, 2106, 2119, 2129, 2209, 2248, 2259, 2260, 2276, 2291,
            2295, 2462, 2809, 2935, 2987, 3098, 3099, 3143, 3146, 3282, 3377, 3378, 3815, 3861, 3897, 3993, 3995, 4093,
            4144, 4156, 4178, 4314, 4388, 4608, 4637, 4642, 4650, 4681, 9276, 9333, 9466, 9468, 9484, 9521, 9522, 9545,
            9568, 9576, 9634, 9689, 9802, 10096, 10255, 10313, 10314, 10906, 11207, 11348, 11530, 11616, 11755, 11863,
            12029, 12200, 12292, 12482, 12519, 12523, 12646, 12825, 12846, 13104
        };

        System.out.println(invalid.length);

        // try {
        // MongoClient mongoClient = new MongoClient("162.105.146.213", 27017);
        // DB db = mongoClient.getDB("NDVR");
        // DBCollection coll = db.getCollection("hsvSignature");
        // BasicDBObject doc = new BasicDBObject("name", "MongoDB")
        // .append("type", "database")
        // .append("count", 1)
        // .append("info",
        // new BasicDBObject("x", 203).append("y", 102));
        //
        // coll.insert(doc);
        // } catch (UnknownHostException e) {
        // logger.error("", e);
        // }
    }

    /**
     * Get all video info from MongoDB, calculate their HSV signatures and save them to MongoDB
     */
    public void save() {
        try {
            MongoClient           mongoClient      = new MongoClient(this.host, this.port);
            DB                    mongoDB          = mongoClient.getDB(this.dbName);
            DBCollection          collection       = mongoDB.getCollection(this.colName);
            VideoInfoDao          videoInfoDao     = new VideoInfoDao();
            List<VideoInfoEntity> videoInfoList    = videoInfoDao.getAllVideoInfo();
            List<String>          invalidVideoList = new ArrayList<String>();    // 生成标签失败的视频

            for (VideoInfoEntity videoEnt : videoInfoList) {
                KeyFrameDao          keyframeDao  = new KeyFrameDao();
                List<KeyFrameEntity> keyframeList = keyframeDao.getKeyFrameByVideoId(videoEnt.getVideoId());
                HSVSigEntity         hsvSig       = GlobalSigGenerator.generate(keyframeList);

                if (null != hsvSig) {
                    VideoHSVSigEntity ent     = new VideoHSVSigEntity(videoEnt.getVideoId(), hsvSig);
                    Gson              gson    = new Gson();
                    String            gsonStr = gson.toJson(ent);

                    System.out.println(gsonStr);

                    DBObject obj = (DBObject) JSON.parse(gsonStr);

                    collection.insert(obj);
                } else {
                    invalidVideoList.add(videoEnt.getVideoId());
                }
            }

            for (String vid : invalidVideoList) {
                System.out.print(vid + ",");
            }
        } catch (UnknownHostException e) {
            logger.error("Unknown MongoDB host. ", e);
        } catch (IOException e) {
            logger.error("IO error. ", e);
        }
    }

    /**
     * Save HSV signatures described in a file.
     */
    public void saveFromFile() {
        try {
            MongoClient    mongoClient = new MongoClient(this.host, this.port);
            DB             mongoDB     = mongoClient.getDB(this.dbName);
            DBCollection   collection  = mongoDB.getCollection(this.colName);
            File           file        = new File(Const.CC_WEB_VIDEO.HSV_SIGNATURE_PATH);
            BufferedReader reader      = null;

            reader = new BufferedReader(new FileReader(file));

            String line = null;

            while ((line = reader.readLine()) != null) {
                logger.info(line);

                String[] info = line.split(" ");

                logger.info(info.length);

                VideoHSVSigEntity ent     = new VideoHSVSigEntity(info);
                Gson              gson    = new Gson();
                String            gsonStr = gson.toJson(ent);

                logger.info(gsonStr);

                DBObject obj = (DBObject) JSON.parse(gsonStr);

                collection.insert(obj);
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
}


//~ Formatted by Jindent --- http://www.jindent.com
