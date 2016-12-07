package com.datalaus.de.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Properties;


public class HBaseUpdateBolt extends BaseRichBolt {
    private static final long serialVersionUID = -5915311156387331493L;

    private static final Logger LOG = Logger.getLogger(HBaseUpdateBolt.class);

    private String[] HBASE_CF;
    private String tableName;

    private final byte[] WORD = Bytes.toBytes("");

    private OutputCollector collector;

    public HBaseUpdateBolt(String habse_cf, String tablename) {
        try {
            tableName = tablename;
            HBASE_CF = habse_cf.split(",");
            HBaseTable.createTable(tableName, HBASE_CF);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

    }

    // @Override
    public void execute(Tuple tuple) {
        // String[] oneLine = tuple.getString(0).split("\t");
        try {
            String word = tuple.getStringByField("word");
            Long count = tuple.getLongByField("count");
            HBaseTable.addRecord(this.tableName, word, "count", "", String.valueOf(count));

        } catch (Exception e) {
            LOG.error("Error inserting data into HBase table", e);
        }

        collector.emit(new Values(tuple.getString(0)));
        // acknowledge even if there is an error
        collector.ack(tuple);
    }

    // @Override
    public void cleanup() {
        try {
            HBaseTable.getAllRecord(this.tableName);
        } catch (Exception e) {
            LOG.error("Error closing connections", e);
        }
    }

    // @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("hbase_entry"));

    }

    // @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public static HBaseUpdateBolt make(Properties topologyConfig) {
        String habse_cf = topologyConfig.getProperty("habse_cf");
        String tablename = topologyConfig.getProperty("hbase_table");
        return new HBaseUpdateBolt(habse_cf, tablename);
    }
}
