package com.datalaus.de.spouts;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import jline.internal.InputStreamReader;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.storm.http.entity.InputStreamEntity;

import backtype.storm.utils.Utils;






public class KafkaFileProducer extends Thread {
	private final KafkaProducer<String, String> producer;
	private final Boolean isAsync;
	private String configFileLocation = "config.properties";
	private Properties properties = new Properties();
	public KafkaFileProducer(String topic, Boolean isAsync) {
		try {
			properties.load(ClassLoader.getSystemResourceAsStream(configFileLocation));
			properties.put("bootstrap.servers", properties.getProperty("bootstrap.servers"));
			properties.put("client.id", properties.getProperty("client.id"));
			properties.put("key.serializer", properties.getProperty("key.serializer"));
			properties.put("value.serializer",properties.getProperty("value.serializer"));
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		producer = new KafkaProducer<String, String>(properties);
        this.isAsync = isAsync;
	}
	
	public void sendMessage(String key, String value) {
		long startTime = System.currentTimeMillis();
		
		if (isAsync) {
			try {
				producer.send(new ProducerRecord<String, String>(properties.getProperty("topicName"), key),
						(Callback) new DemoCallBack(startTime, key, value));
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}
		} else {
			try {
				producer.send(new ProducerRecord<String, String>(properties.getProperty("topicName"), key, value)).get();
				System.out.println("Send message: ("+ key + ", " + value + ")");
				Utils.sleep(360 * 100);
			} catch ( InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void run() {
		KafkaFileProducer kafkaFileProducer = new KafkaFileProducer(properties.getProperty("topicName"), false);
		int lineCount = 0;
		FileInputStream fileInputStream;
		BufferedReader bufferedReader = null;
		try {
			fileInputStream = new FileInputStream(properties.getProperty("fileName"));
			bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
			
			String line = null;
			while ((line = bufferedReader.readLine()) != null) {
				lineCount++;
				kafkaFileProducer.sendMessage(lineCount+"", line);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				bufferedReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}

class DemoCallBack implements Callback {

    private long startTime;
    private String key;
    private String message;

    public DemoCallBack(long startTime, String key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling
     * of request completion. This method will be called when the record sent to
     * the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata
     *            The metadata for the record that was sent (i.e. the partition
     *            and offset). Null if an error occurred.
     * @param exception
     *            The exception thrown during processing of this record. Null if
     *            no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            System.out.println("message(" + key + ", " + message
                    + ") sent to partition(" + metadata.partition() + "), "
                    + "offset(" + metadata.offset() + ") in " + elapsedTime
                    + " ms");
        } else {
            exception.printStackTrace();
        }
    }
}