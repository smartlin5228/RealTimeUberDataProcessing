package com.datalaus.de.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by cloudera on 12/16/16.
 */
public class RedisBolt extends IRichBolt  {
    protected String channel = "data";
    protected OutputCollector outputCollector;
    protected JedisPool jedisPool;

    public RedisBolt(){}
    public RedisBolt(String channel) {

    }

    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        jedisPool = new JedisPool(new JedisPoolConfig(), "0.0.0.0");
    }

    public void execute(Tuple tuple) {
        String current = tuple.getString(0);
        if(current != null) {
            //	    for(Object obj: result) {
            publish(current);
            outputCollector.emit(tuple, new Values(current));
            //	    }
            outputCollector.ack(tuple);
        }
    }

    private void publish(String msg) {
        Jedis jedis = jedisPool.getResource();
        jedis.publish(channel, msg);
        jedisPool.returnResource(jedis);
    }

    public void cleanup() {
        if (jedisPool != null) {
            jedisPool.destroy();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(channel));
    }
    public Map getComponentConfiguration() {
        return null;
    }
}
