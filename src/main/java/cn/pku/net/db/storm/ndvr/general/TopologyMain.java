/**
 * @Package cn.pku.net.db.storm.ndvr.topology
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.general;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import clojure.core.Vec;
import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.dao.HSVSignatureDao;
import cn.pku.net.db.storm.ndvr.dao.VideoInfoDao;
import cn.pku.net.db.storm.ndvr.entity.VideoHSVSigEntity;
import cn.pku.net.db.storm.ndvr.entity.VideoInfoEntity;
import cn.pku.net.db.storm.ndvr.lsh.*;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.List;

import static cn.pku.net.db.storm.ndvr.common.Const.SSM_CONFIG.*;

/**
 * Description: General Storm Topology
 *
 * @author jeremyjiang
 * Created at 2016/5/12 18:31
 */
public class TopologyMain {
    private static final Logger logger = Logger.getLogger(TopologyMain.class);

    public static LSH globalVisualLSH;

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {

        //int radiusEuclidean = 2;
        //int dimension = 24;
        //HashFamily euclideanHashFamily = new EuclideanHashFamily(radiusEuclidean, dimension);
        //List<Vector> dataset = Mongo2Vec.getAllGlobalSigAsVec();
        //globalVisualLSH = new LSH(dataset, euclideanHashFamily);
        //int numOfHashes = 64;
        //int numberOfHashTables = 4;
        //globalVisualLSH.buildIndex(numOfHashes, numberOfHashTables);
        //
        //HSVSignatureDao hsvSigDao = new HSVSignatureDao();
        //VideoInfoDao videoInfoDao = new VideoInfoDao();
        //VideoInfoEntity queryVideo = videoInfoDao.getVideoInfoById("1");
        //VideoHSVSigEntity queryVideoSig = hsvSigDao.getVideoHSVSigById("1");
        //Vector queryVec = Mongo2Vec.convert(queryVideoSig);
        //
        //int maxSize = 500;
        //List<Vector> candidates = globalVisualLSH.query(queryVec, maxSize);
        //
        //System.out.println("Candidates size: " + candidates.size());
        //
        //int queryDuration = queryVideo.getDuration();
        //
        //List<VideoInfoEntity> candidates2 = videoInfoDao.getVideoInfoByDuration(queryDuration - Const.STORM_CONFIG.VIDEO_DURATION_WINDOW,
        //            queryDuration + Const.STORM_CONFIG.VIDEO_DURATION_WINDOW );
        //System.out.println("By duration, Candidates size: " + candidates2.size());

        // the 1th application, pre retrieval, textual and global visual signature, set IS_FILTER_AND_REFINE to true
        // the 2th application, post retrieval, textual and global visual signature, set IS_FILTER_AND_REFINE to false
        STORM_TOPOLOGY.put("GetTaskSpout", new HashSet<String>(){ { add("TextSimilarBolt"); } });
        STORM_TOPOLOGY.put("TextSimilarBolt", new HashSet<String>(){ { add("GlobalSigBolt"); } });
        STORM_TOPOLOGY.put("GlobalSigBolt", new HashSet<String>(){ { add("GlobalSigSimilarBolt"); } });
        STORM_TOPOLOGY.put("GlobalSigSimilarBolt", new HashSet<String>(){ { add("TextGlobalPreResultBolt"); } });
        STORM_TOPOLOGY.put("TextGlobalPreResultBolt", new HashSet<String>());
        // define the needed fields of each component
        NEED_KEYS.put("GetTaskSpout", new HashSet<String>(){ { add("task"); } });
        NEED_KEYS.put("TextSimilarBolt", new HashSet<String>(){ { add("queryVideo"); } });
        NEED_KEYS.put("GlobalSigBolt", new HashSet<String>(){ { add("queryVideo"); } });
        NEED_KEYS.put("GlobalSigSimilarBolt", new HashSet<String>(){ { add("queryVideo"); add("textSimilarVideoList"); add("globalSignature"); } });
        NEED_KEYS.put("TextGlobalPreResultBolt", new HashSet<String>(){ { add("globalSimilarVideoList"); } });
        // define the produced fields of each component
        NEW_KEYS.put("GetTaskSpout", new HashSet<String>(){ { add("queryVideo"); } });
        NEW_KEYS.put("TextSimilarBolt", new HashSet<String>(){ { add("textSimilarVideoList"); } });
        NEW_KEYS.put("GlobalSigBolt", new HashSet<String>(){ { add("globalSignature"); add("keyframeList"); } });
        NEW_KEYS.put("GlobalSigSimilarBolt", new HashSet<String>(){ { add("globalSimilarVideoList"); } });
        NEW_KEYS.put("TextGlobalPreResultBolt", new HashSet<String>(){ { add("result"); } });
        DISCARD_KEYS = StreamSharedMessage.calMsgReduction(STORM_TOPOLOGY, NEED_KEYS, NEW_KEYS);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("getTask", new GetTaskSpout(), 1);
        builder.setBolt("textSimilarity", new TextSimilarBolt(), 100).fieldsGrouping("getTask", new Fields("fieldGroupingId"));
        builder.setBolt("globalSigBolt", new GlobalSigBolt(), 100).fieldsGrouping("textSimilarity", new Fields("fieldGroupingId"));
        builder.setBolt("globalSigSimilarBolt", new GlobalSigSimilarBolt(), 100).fieldsGrouping("globalSigBolt", new Fields("fieldGroupingId"));
        builder.setBolt("TextGlobalPreResultBolt", new TextGlobalPreResultBolt(), 10).fieldsGrouping("globalSigSimilarBolt", new Fields("taskId"));

        // the 3th application, pre retrieval, global and local visual signature, set IS_FILTER_AND_REFINE to true
        //STORM_TOPOLOGY.put("GetTaskSpout", new HashSet<String>(){ { add("GlobalSigBolt"); } });
        //STORM_TOPOLOGY.put("GlobalSigBolt", new HashSet<String>(){ { add("GlobalSigSimilarBolt"); } });
        //STORM_TOPOLOGY.put("GlobalSigSimilarBolt", new HashSet<String>(){ { add("LocalSigBolt"); } });
        //STORM_TOPOLOGY.put("LocalSigBolt", new HashSet<String>(){ { add("LocalSigSimilarBolt"); } });
        //STORM_TOPOLOGY.put("LocalSigSimilarBolt", new HashSet<String>(){ { add("GlobalLocalPreResultBolt"); } });
        //STORM_TOPOLOGY.put("GlobalLocalPreResultBolt", new HashSet<String>());
        //// define the needed fields of each component
        //NEED_KEYS.put("GetTaskSpout", new HashSet<String>(){ { add("task"); } });
        //NEED_KEYS.put("GlobalSigBolt", new HashSet<String>(){ { add("queryVideo"); } });
        //NEED_KEYS.put("GlobalSigSimilarBolt", new HashSet<String>(){ { add("queryVideo"); add("globalSignature"); } });
        //NEED_KEYS.put("LocalSigBolt", new HashSet<String>(){ { add("queryVideo"); add("keyframeList"); } });
        //NEED_KEYS.put("LocalSigSimilarBolt", new HashSet<String>(){ { add("queryVideo"); add("localSignature"); } });
        //NEED_KEYS.put("GlobalLocalPreResultBolt", new HashSet<String>(){ { add("localSimilarVideoList"); } });
        //// define the produced fields of each component
        //NEW_KEYS.put("GetTaskSpout", new HashSet<String>(){ { add("queryVideo"); } });
        //NEW_KEYS.put("GlobalSigBolt", new HashSet<String>(){ { add("keyframeList"); add("globalSignature"); } });
        //NEW_KEYS.put("GlobalSigSimilarBolt", new HashSet<String>(){ { add("globalSimilarVideoList"); } });
        //NEW_KEYS.put("LocalSigBolt", new HashSet<String>(){ { add("localSignature"); } });
        //NEW_KEYS.put("LocalSigSimilarBolt", new HashSet<String>(){ { add("localSimilarVideoList"); } });
        //NEW_KEYS.put("GlobalLocalPreResultBolt", new HashSet<String>(){ { add("result"); } });
        //DISCARD_KEYS = StreamSharedMessage.calMsgReduction(STORM_TOPOLOGY, NEED_KEYS, NEW_KEYS);
        //TopologyBuilder builder = new TopologyBuilder();
        //builder.setSpout("getTask", new GetTaskSpout(), 1);
        //builder.setBolt("globalSigBolt", new GlobalSigBolt(), 100).fieldsGrouping("getTask", new Fields("fieldGroupingId"));
        //builder.setBolt("globalSigSimilarBolt", new GlobalSigSimilarBolt(), 100).fieldsGrouping("globalSigBolt", new Fields("fieldGroupingId"));
        //builder.setBolt("localSigBolt", new LocalSigBolt(), 100).fieldsGrouping("globalSigSimilarBolt", new Fields("fieldGroupingId"));
        //builder.setBolt("localSigSimilarBolt", new LocalSigSimilarBolt(), 100).fieldsGrouping("localSigBolt", new Fields("fieldGroupingId"));
        //builder.setBolt("globalLocalPreResultBolt", new GlobalLocalPreResultBolt(), 1).fieldsGrouping("localSigSimilarBolt", new Fields("fieldGroupingId"));

        // the 4th application, detection, textual signature
        // define the Storm topology
        //STORM_TOPOLOGY.put("GetTaskSpout", new HashSet<String>(){ { add("TextSimilarBolt"); } });
        //STORM_TOPOLOGY.put("TextSimilarBolt", new HashSet<String>(){ { add("TextResultBolt"); } });
        //STORM_TOPOLOGY.put("TextSimilarBolt", new HashSet<String>(){ { add("TextResultBolt"); } });
        //STORM_TOPOLOGY.put("TextResultBolt", new HashSet<String>());
        //// define the needed fields of each component
        //NEED_KEYS.put("GetTaskSpout", new HashSet<String>(){ { add("task"); } });
        //NEED_KEYS.put("TextSimilarBolt", new HashSet<String>(){ { add("queryVideo1"); add("queryVideo2"); } });
        //NEED_KEYS.put("TextResultBolt", new HashSet<String>(){ { add("queryVideo1"); add("queryVideo2"); add("textSimilarity"); } });
        //// define the produced fields of each component
        //NEW_KEYS.put("GetTaskSpout", new HashSet<String>(){ { add("queryVideo1"); add("queryVideo2"); } });
        //NEW_KEYS.put("TextSimilarBolt", new HashSet<String>(){ { add("textSimilarity"); } });
        //NEW_KEYS.put("TextResultBolt", new HashSet<String>(){ { add("result"); } });
        //DISCARD_KEYS = StreamSharedMessage.calMsgReduction(STORM_TOPOLOGY, NEED_KEYS, NEW_KEYS);
        //TopologyBuilder builder = new TopologyBuilder();
        //builder.setSpout("getTask", new GetTaskSpout(), 1);
        //builder.setBolt("textSimilarity", new TextSimilarBolt(), 100).fieldsGrouping("getTask", new Fields("fieldGroupingId"));
        //builder.setBolt("result", new TextResultBolt(), 10).fieldsGrouping("textSimilarity", new Fields("taskId"));

        // the 5nd application, post detection, textual and global visual signature, set IS_FILTER_AND_REFINE to false
        // the 6rd application, pre detection, textual and global visual signature, set IS_FILTER_AND_REFINE to true
        //STORM_TOPOLOGY.put("GetTaskSpout", new HashSet<String>(){ { add("TextSimilarBolt"); } });
        //STORM_TOPOLOGY.put("TextSimilarBolt", new HashSet<String>(){ { add("GlobalSigBolt"); } });
        //STORM_TOPOLOGY.put("GlobalSigBolt", new HashSet<String>(){ { add("GlobalSigSimilarBolt"); } });
        //STORM_TOPOLOGY.put("GlobalSigSimilarBolt", new HashSet<String>(){ { add("TextGlobalPostResultBolt"); } });
        //STORM_TOPOLOGY.put("TextGlobalPostResultBolt", new HashSet<String>());
        //// define the needed fields of each component
        //NEED_KEYS.put("GetTaskSpout", new HashSet<String>(){ { add("task"); } });
        //NEED_KEYS.put("TextSimilarBolt", new HashSet<String>(){ { add("queryVideo1"); add("queryVideo2"); } });
        //NEED_KEYS.put("GlobalSigBolt", new HashSet<String>(){ { add("queryVideo1"); add("queryVideo2"); } });
        //NEED_KEYS.put("GlobalSigSimilarBolt", new HashSet<String>(){ { add("globalSignature1"); add("globalSignature2"); } });
        //NEED_KEYS.put("TextGlobalPostResultBolt", new HashSet<String>(){ { add("queryVideo1"); add("queryVideo2"); add("textSimilarity"); add("globalDistance"); } });
        //// define the produced fields of each component
        //NEW_KEYS.put("GetTaskSpout", new HashSet<String>(){ { add("queryVideo1"); add("queryVideo2"); } });
        //NEW_KEYS.put("TextSimilarBolt", new HashSet<String>(){ { add("textSimilarity"); } });
        //NEW_KEYS.put("GlobalSigBolt", new HashSet<String>(){ { add("keyframeList1"); add("keyframeList2"); add("globalSignature1"); add("globalSignature2"); } });
        //NEW_KEYS.put("GlobalSigSimilarBolt", new HashSet<String>(){ { add("globalDistance"); } });
        //NEW_KEYS.put("TextGlobalPostResultBolt", new HashSet<String>(){ { add("result"); } });
        //DISCARD_KEYS = StreamSharedMessage.calMsgReduction(STORM_TOPOLOGY, NEED_KEYS, NEW_KEYS);
        //TopologyBuilder builder = new TopologyBuilder();
        //builder.setSpout("getTask", new GetTaskSpout(), 1);
        //builder.setBolt("textSimilarity", new TextSimilarBolt(), 100).fieldsGrouping("getTask", new Fields("fieldGroupingId"));
        //builder.setBolt("globalSigBolt", new GlobalSigBolt(), 100).fieldsGrouping("textSimilarity", new Fields("fieldGroupingId"));
        //builder.setBolt("globalSigSimilarBolt", new GlobalSigSimilarBolt(), 100).fieldsGrouping("globalSigBolt", new Fields("fieldGroupingId"));
        //builder.setBolt("textGlobalPostResultBolt", new TextGlobalPostResultBolt(), 10).fieldsGrouping("globalSigSimilarBolt", new Fields("taskId"));


        Config conf = new Config();
        conf.setNumWorkers(50);
        conf.setMaxSpoutPending(5000);
        conf.setDebug(false);
        if (Const.STORM_CONFIG.IS_LOCAL_MODE) {    // 如果是本地模式
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("ndvr", conf, builder.createTopology());
        } else {                                   // 如果在集群上运行
            try {
                StormSubmitter.submitTopology("ndvr", conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                logger.error("The topology is already alive! " + e.getMessage());
            } catch (InvalidTopologyException e) {
                logger.error("InvalidTopology! " + e.getMessage());
            }
        }
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
