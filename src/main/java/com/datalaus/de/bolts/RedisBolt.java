package com.datalaus.de.bolts;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * This abstract bolt class will help publishing the bolt processing into
 * a redis pubsub channel
 *
 *
 */
public class RedisBolt implements IRichBolt {

    protected String channel = "data";
    protected OutputCollector collector;
    protected JedisPool pool;

    public RedisBolt(){}
    public RedisBolt(String channel) {

    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        pool = new JedisPool(new JedisPoolConfig(), "0.0.0.0");
    }

    public void execute(Tuple tuple) {
        String current = tuple.getString(0);
        if(current != null) {
            //	    for(Object obj: result) {
            publish(current);
            collector.emit(tuple, new Values(current));
            //	    }
            collector.ack(tuple);
        }
    }

    public void cleanup() {
        if(pool != null) {
            pool.destroy();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(channel));
    }

    public void publish(String msg) {
        Jedis jedis = pool.getResource();
        jedis.publish(channel, msg);
        pool.returnResource(jedis);
    }

    protected void setupNonSerializableAttributes() {

    }

    public Map getComponentConfiguration() {
        return null;
    }
}