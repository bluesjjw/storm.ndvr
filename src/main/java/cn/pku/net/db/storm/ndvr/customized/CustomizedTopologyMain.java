/**
 * @Title: CustomizedTextTopologyMain.java 
 * @Package cn.pku.net.db.storm.ndvr.customized.text 
 * @Description: TODO
 * @author Jiawei Jiang    
 * @date 2015年1月30日 下午4:00:19 
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved.
 */
package cn.pku.net.db.storm.ndvr.customized;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import cn.pku.net.db.storm.ndvr.common.Const;

/**
 * @ClassName: CustomizedTextTopologyMain 
 * @Description: TODO
 * @author Jiawei Jiang
 * @date 2015年1月30日 下午4:00:19
 */
public class CustomizedTopologyMain {

    private static final Logger logger = Logger.getLogger(CustomizedTopologyMain.class);

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
        builder.setSpout("getTask", new CustomizedDetectionSpout(), 1);

        //text
        //        builder.setBolt("feature", new CustomizedTextDetectionBolt(), 100).shuffleGrouping(
        //            "getTask");
        builder.setBolt("similarity", new CustomizedTextDetectionBolt(), 400).shuffleGrouping(
            "getTask");
        builder.setBolt("result", new CustomizedTextDetectionResult(), 100).fieldsGrouping(
            "similarity", new Fields("taskId"));

        Config conf = new Config();
        conf.setNumWorkers(600);
        conf.setMaxSpoutPending(5000);
        conf.setDebug(false);

        if (Const.STORM_CONFIG.IS_LOCAL_MODE) { //如果是本地模式
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("customizedRetrieval", conf, builder.createTopology());
        } else { //如果在集群上运行
            try {
                StormSubmitter
                    .submitTopology("customizedRetrieval", conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                logger.error("The topology is already alive! " + e.getMessage());
            } catch (InvalidTopologyException e) {
                logger.error("InvalidTopology! " + e.getMessage());
            }
        }
    }
}
