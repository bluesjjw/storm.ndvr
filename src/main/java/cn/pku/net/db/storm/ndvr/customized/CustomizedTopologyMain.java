/**
 * @Package cn.pku.net.db.storm.ndvr.customized
 * Created by jeremyjiang on 2016/5/12.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
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
 * Description: Customized Storm topology
 * @author jeremyjiang
 * Created at 2016/5/12 20:53
 */
public class CustomizedTopologyMain {

    private static final Logger logger = Logger.getLogger(CustomizedTopologyMain.class);

    public static void main(String[] args) {

        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("getTask", new CusDetecSpout(), 1);

        //text
        //        builder.setBolt("feature", new CusTextDetecBolt(), 100).shuffleGrouping(
        //            "getTask");
        builder.setBolt("similarity", new CusTextDetecBolt(), 400).shuffleGrouping(
            "getTask");
        builder.setBolt("result", new CusTextDetecResult(), 100).fieldsGrouping(
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
