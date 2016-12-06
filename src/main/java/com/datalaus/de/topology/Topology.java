package com.datalaus.de.topology;

import java.io.Serializable;
import java.util.Properties;
import java.util.UUID;

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
            topologyBuilder.setBolt("WordSplitBolt", new WordSplitterBolt(5)).shuffleGrouping("batchFileSpout");
		} catch (Exception e) {
			// TODO: handle exception
		}
	}
}
