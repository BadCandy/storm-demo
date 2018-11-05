package me.christ9979.demostorm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class WordCountBolt extends BaseRichBolt {

    private OutputCollector outputCollector;
    private Map<String, Integer> wordCountMap;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        wordCountMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {

        String word = tuple.getStringByField("word");
        countWord(word);
        outputCollector.emit(tuple, new Values(word, wordCountMap.get(word)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "count"));
    }

    private void countWord(String... words) {

        Arrays.stream(words).forEach(word -> {
            if (!wordCountMap.containsKey(word)) {
                wordCountMap.put(word, 1);
            } else {
                wordCountMap.put(word, wordCountMap.get(word) + 1);
            }
        });
    }
}
