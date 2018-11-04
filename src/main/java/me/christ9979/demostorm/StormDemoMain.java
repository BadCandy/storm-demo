package me.christ9979.demostorm;

import me.christ9979.demostorm.bolt.DoubleAndTripleBolt;
import me.christ9979.demostorm.spout.DoubleAndTripleSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class StormDemoMain {

    public static void main(String[] args) {

        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(3);

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("spout1", new DoubleAndTripleSpout(), 10);
        topologyBuilder.setBolt("bolt2", new DoubleAndTripleBolt(), 3).shuffleGrouping("spout1");

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("test", config, topologyBuilder.createTopology());
        Utils.sleep(10000);
        localCluster.killTopology("test");
        localCluster.shutdown();

    }
}
