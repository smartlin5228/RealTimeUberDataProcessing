package com.datalaus.de.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by cloudera on 12/5/16.
 */
public class WordSplitterBolt extends BaseRichBolt {
    private static final long serialVersionUID = 5151173513759399636L;
    private final int minWordLength;
    private OutputCollector outputCollector;
    public WordSplitterBolt(int minWordLength) {
        this.minWordLength = minWordLength;
    }
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }
    public void execute(Tuple input) {
        String review = input.getString(0);
        String text = review.replaceAll("\\p{Punct}", " ").replaceAll("\\r|\\n", "").toLowerCase();
        String[] words = text.split(" ");
        for (String word : words) {
            if (word.length() >= minWordLength) {
                outputCollector.emit(new Values(word));
                System.out.println(word);
            }
        }
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
