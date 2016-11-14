/**
 * Created by jeremyjiang on 2016/6/8.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */
package cn.pku.net.db.storm.ndvr.distribute;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import cn.pku.net.db.storm.ndvr.common.Const;
import org.apache.log4j.Logger;

/**
 * Description: Topology for distributed NDV system
 *
 * @author jeremyjiang
 * Created at 2016/6/8 9:16
 */

public class TopologyMainDistributed {

    private static final Logger logger = Logger.getLogger(TopologyMainDistributed.class);

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {
        // the 1st application, detection, textual signature
        // define the Storm topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("taskScheduler", new RetriTaskScheduler(), 1);
        builder.setBolt("textSimilarity", new DisTextRetri(), 100).shuffleGrouping("taskScheduler");

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