/**
 * @Package cn.pku.net.db.storm.ndvr.topology
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */



package cn.pku.net.db.storm.ndvr.topology;

import cn.pku.net.db.storm.ndvr.bolt.*;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import cn.pku.net.db.storm.ndvr.bolt.GlobalFeatureBolt;
import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.spout.GetTaskSpout;

/**
 * Description: General Storm Topology
 *
 * @author jeremyjiang
 * Created at 2016/5/12 18:31
 */
public class TopologyMain {
    private static final Logger logger = Logger.getLogger(TopologyMain.class);

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {

        // Topology definition
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("getTask", new GetTaskSpout(), 1);

        // builder.setBolt("textSimilarity", new TextSimilarityRetrievalBolt(), 100).fieldsGrouping(
        // "getTask", new Fields("fieldGroupingId"));
        builder.setBolt("globalFeature", new GlobalFeatureBolt(), 100)
               .fieldsGrouping("getTask", new Fields("fieldGroupingId"));
        builder.setBolt("globalDistance", new GlobalSigDistanceBolt(), 100)
               .fieldsGrouping("globalFeature", new Fields("fieldGroupingId"));
        builder.setBolt("localFeature", new LocalFeatureRetrievalBolt(), 100).shuffleGrouping("globalDistance");
        builder.setBolt("localDistance", new LocalSigDistanceRetrievalBolt(), 100).shuffleGrouping("localFeature");
        builder.setBolt("result", new Algorithm3ResultBolt(), 20).shuffleGrouping("localDistance");

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
