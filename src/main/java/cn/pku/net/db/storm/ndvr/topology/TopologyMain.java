/**
 * @Title: TopologyMain.java 
 * @Package cn.pku.net.db.storm.ndvr.topology 
 * @Description: TODO
 * @author Jiawei Jiang    
 * @date 2015年1月3日 上午9:37:09 
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved.
 */
package cn.pku.net.db.storm.ndvr.topology;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import cn.pku.net.db.storm.ndvr.bolt.Algorithm3ResultBolt;
import cn.pku.net.db.storm.ndvr.bolt.GlobalFeatureRetrievalBolt;
import cn.pku.net.db.storm.ndvr.bolt.GlobalSigDistanceRetrievalBolt;
import cn.pku.net.db.storm.ndvr.bolt.LocalFeatureRetrievalBolt;
import cn.pku.net.db.storm.ndvr.bolt.LocalSigDistanceRetrievalBolt;
import cn.pku.net.db.storm.ndvr.common.Const;
import cn.pku.net.db.storm.ndvr.spout.GetTaskSpout;

/**
 * @ClassName: TopologyMain 
 * @Description: TODO
 * @author Jiawei Jiang
 * @date 2015年1月3日 上午9:37:09
 */
public class TopologyMain {

    private static final Logger logger = Logger.getLogger(TopologyMain.class);

    /**
     * @Title: main 
     * @Description: TODO
     * @param @param args     
     * @return void   
     * @throws 
     * @param args
     */
    public static void main(String[] args) {

        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("getTask", new GetTaskSpout(), 1);

        //        builder.setBolt("textSimilarity", new TextSimilarityRetrievalBolt(), 100).fieldsGrouping(
        //            "getTask", new Fields("fieldGroupingId"));
        builder.setBolt("globalFeature", new GlobalFeatureRetrievalBolt(), 100).fieldsGrouping(
            "getTask", new Fields("fieldGroupingId"));
        builder.setBolt("globalDistance", new GlobalSigDistanceRetrievalBolt(), 100)
            .fieldsGrouping("globalFeature", new Fields("fieldGroupingId"));
        builder.setBolt("localFeature", new LocalFeatureRetrievalBolt(), 100).shuffleGrouping(
            "globalDistance");
        builder.setBolt("localDistance", new LocalSigDistanceRetrievalBolt(), 100).shuffleGrouping(
            "localFeature");
        builder.setBolt("result", new Algorithm3ResultBolt(), 20).shuffleGrouping("localDistance");

        Config conf = new Config();
        conf.setNumWorkers(50);
        conf.setMaxSpoutPending(5000);
        conf.setDebug(false);

        if (Const.STORM_CONFIG.IS_LOCAL_MODE) { //如果是本地模式
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("ndvr", conf, builder.createTopology());
        } else { //如果在集群上运行
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
