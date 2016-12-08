package com.datalaus.de.topology;

import java.io.Serializable;
import java.util.Properties;
import java.util.UUID;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.utils.Utils;
import com.datalaus.de.bolts.HBaseUpdateBolt;
import com.datalaus.de.bolts.WordCounterBolt;
import com.datalaus.de.bolts.WordSplitterBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;

import com.datalaus.de.spouts.KafkaFileProducer;



public class Topology implements Serializable{
	private static final Logger LOGGER = LoggerFactory.getLogger(Topology.class);
	static final String TOPOLOGY_NAME = "storm-kafka-uber-data-word count";
	public static final void main(final String[] args) {
		try {
			//configuration
			String configFileLocation = "config.properties";
			Properties topologyConfig = new Properties();
			topologyConfig.load(ClassLoader.getSystemResourceAsStream(configFileLocation));
			//properties retrieved from property file
			String kafkaServer = topologyConfig.getProperty("kafkaserver");
			String zookeeperConnection = topologyConfig.getProperty("zookeeper");
			String topicName = topologyConfig.getProperty("topic");
			//kafka file producer
			KafkaFileProducer kafkaFileProducer = new KafkaFileProducer(topicName, false);
			kafkaFileProducer.start();
			//Config for building the topology
			final Config config = new Config();
			config.setMessageTimeoutSecs(20);
			TopologyBuilder topologyBuilder = new TopologyBuilder();
			//Topology components
			BrokerHosts hosts = new ZkHosts(zookeeperConnection);
			SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
			spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
            KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
            //Set Components
			topologyBuilder.setSpout("batchFileSpout", kafkaSpout, 1);
			topologyBuilder.setBolt("WordSplitterBolt", new WordSplitterBolt(5)).shuffleGrouping("batchFileSpout");
			topologyBuilder.setBolt("WordCounterBolt", new WordCounterBolt(10, 5 * 60, 50)).shuffleGrouping("WordSplitterBolt");
			//add hbasebolt
			//topologyBuilder.setBolt("redis", new RedisBolt()).shuffleGrouping("WordCounterBolt");
			topologyBuilder.setBolt("HbaseBolt", HBaseUpdateBolt.make(topologyConfig)).shuffleGrouping("WordCounterBolt");
			if (null != args && 0 < args.length) {
				config.setNumWorkers(3);
				StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
			} else {
				config.setMaxTaskParallelism(10);
				final LocalCluster localCluster = new LocalCluster();
				localCluster.submitTopology(TOPOLOGY_NAME, config, topologyBuilder.createTopology());

				Utils.sleep(360 * 10000);

				LOGGER.info("Shutting down the cluster");
				localCluster.killTopology(TOPOLOGY_NAME);
				localCluster.shutdown();
			}
		} catch (final InvalidTopologyException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
