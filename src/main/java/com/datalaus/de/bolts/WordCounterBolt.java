package com.datalaus.de.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by cloudera on 12/6/16.
 */
public class WordCounterBolt extends BaseRichBolt {
    private static final long serialVersionUID = 2706047697068872387L;
    private static final Logger logger = LoggerFactory.getLogger(WordCounterBolt.class);

    private final long logIntervalSec;
    private final long clearIntervalSec;

    private final int topListSize;

    private Map<String, Long> counter;
    private long lastLogTime;
    private long lastClearTime;

    private OutputCollector collector;

    public WordCounterBolt(long logIntervalSec, long clearIntervalSec, int topListSize) {
        this.logIntervalSec = logIntervalSec;
        this.clearIntervalSec = clearIntervalSec;
        this.topListSize = topListSize;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        counter = new HashMap<String, Long>();
        lastClearTime = System.currentTimeMillis();
        lastLogTime = System.currentTimeMillis();
        this.collector = outputCollector;
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "count"));
    }

    public void execute(Tuple input) {
        String word = (String) input.getValueByField("word");
        Long count = counter.get(word) == null ? 1l:counter.get(word);
        count = count == null ? 1L : count + 1;
        counter.put(word, count);
        collector.emit(new Values(word, count));

        long now = System.currentTimeMillis();
        long logPeriodSec = (now - lastLogTime) / 1000;
        if (logPeriodSec > logIntervalSec) {
            logger.info("\n\n");
            logger.info("Word count: " + counter.size());

            publishTopList();
            lastLogTime = now;
        }
    }

    private void publishTopList() {
        SortedMap<Long, String> top = new TreeMap<Long, String>();
        for (Map.Entry<String, Long> entry : counter.entrySet()) {
            long count = entry.getValue();
            String word = entry.getKey();

            top.put(count, word);
            if (top.size() > topListSize) {
                top.remove(top.firstKey());
            }
        }

        // Output top list:
        for (Map.Entry<Long, String> entry : top.entrySet()) {
            logger.info(new StringBuilder("top - ").append(entry.getValue()).append('|').append(entry.getKey()).toString());
        }

        // Clear top list
        long now = System.currentTimeMillis();
        if (now - lastClearTime > clearIntervalSec * 1000) {
            counter.clear();
            lastClearTime = now;
        }
    }
}
