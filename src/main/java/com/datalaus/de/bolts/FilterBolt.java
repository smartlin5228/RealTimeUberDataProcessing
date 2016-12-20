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
 * Created by cloudera on 12/16/16.
 */
public class FilterBolt extends BaseRichBolt {
    private static final long serialVersionUID = 5151173513759399636L;

    private final int minWordLength;

    private OutputCollector outputCollector;

    public FilterBolt(int minWordLength) {
        this.minWordLength = minWordLength;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    public void execute(Tuple input) {
        String review = input.getString(0);
        String[] words = review.split(",");
        if (words.length == 4) {
            outputCollector.emit(new Values(words[0] + ' ' + words[1] + ' ' + words[2] + ' '+ words[3]));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
