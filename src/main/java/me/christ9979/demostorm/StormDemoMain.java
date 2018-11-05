package me.christ9979.demostorm;

import me.christ9979.demostorm.bolt.DoubleAndTripleBolt;
import me.christ9979.demostorm.bolt.ReportBolt;
import me.christ9979.demostorm.bolt.SentenceSplitBolt;
import me.christ9979.demostorm.bolt.WordCountBolt;
import me.christ9979.demostorm.spout.DoubleAndTripleSpout;
import me.christ9979.demostorm.spout.SentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class StormDemoMain {

    public static void main(String[] args) {

        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(3);

        StormTopology topology = buildWordCountTopology();

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("test", config, topology);
        Utils.sleep(3000);
        localCluster.killTopology("test");
        localCluster.shutdown();

    }

    private static StormTopology buildDoubleAndTripleTopology() {

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("spout1", new DoubleAndTripleSpout(), 10);
        topologyBuilder.setBolt("bolt2", new DoubleAndTripleBolt(), 3).shuffleGrouping("spout1");

        return topologyBuilder.createTopology();
    }

    private static StormTopology buildWordCountTopology() {

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("sentenceSpout", new SentenceSpout(), 2);
        topologyBuilder.setBolt("sentenceSplitBolt", new SentenceSplitBolt(), 3).shuffleGrouping("sentenceSpout");
        topologyBuilder.setBolt("wordCountBolt", new WordCountBolt(), 3).fieldsGrouping("sentenceSplitBolt", new Fields("word"));
        topologyBuilder.setBolt("reportBolt", new ReportBolt(), 3).shuffleGrouping("wordCountBolt");


        return topologyBuilder.createTopology();
    }


}
