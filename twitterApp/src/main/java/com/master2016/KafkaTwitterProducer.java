package com.master2016;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.I0Itec.zkclient.ZkClient;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;


import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.zookeeper.server.auth.IPAuthenticationProvider;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

public class KafkaTwitterProducer {
	
	private HashSet<String> currentTopics;
	private PrintWriter printWriter;
	KafkaProducer<String, String> producer;

	public KafkaTwitterProducer() {
		currentTopics = getCurrentTopics();
		load();
	}

	public void fromFile() throws IOException, InterruptedException {

		FileReader fr = new FileReader(Keys.fileName);
		BufferedReader br = new BufferedReader(fr);
		String linea;
		ProducerRecord<String, String> message = null;
		Status status = null;
		
		try {
			linea = br.readLine();
			while (linea != null) {	
				try {
					System.out.println(linea);

					status = TwitterObjectFactory.createStatus(linea);
					
					if(currentTopics.contains(status.getLang())){
						message = new ProducerRecord<String, String>(status.getLang(), linea);
						
						producer.send(message);
					}

				} catch(TwitterException e) {
					System.out.println(e.toString());
				}	
				System.out.println(linea);
				
				linea = br.readLine();
			}
		} catch (Exception ex) {
			System.out.println(ex);
			throw ex;
		} finally {
			br.close();
			fr.close();
			producer.close();
		}

	}

	public void fromTwitter() throws InterruptedException {

		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(100000);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		endpoint.trackTerms(Lists.newArrayList("twitter"));
		ProducerRecord<String, String> message = null;
		Status status = null;
		String statusString = null;

		String consumerKey = Keys.apiKey;
		String consumerSecret = Keys.apiSecret;
		String accessToken = Keys.tokenValue;
		String accessTokenSecret = Keys.tokenSecret;

		Authentication auth = new OAuth1(consumerKey, consumerSecret, accessToken, accessTokenSecret);

		Client client = new ClientBuilder().hosts(Constants.STREAM_HOST).endpoint(endpoint).authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();

		try{
			client.connect();
			for (int msgRead = 0; msgRead < 1000; msgRead++) {
				try {
					System.out.println(queue.take());
					statusString = queue.take();

					status = TwitterObjectFactory.createStatus(statusString);
					
					if(currentTopics.contains(status.getLang())){
						message = new ProducerRecord<String, String>(status.getLang(), statusString);
						
						producer.send(message);
//						printToFile(statusString);
					}

				} catch (InterruptedException | TwitterException e) {
					//System.out.println(e.toString());
				}

			}
		}catch(Exception ex){
			
		}finally {
			producer.close();
			client.stop();
		}

	}
	
	private HashSet<String> getCurrentTopics(){
		
		HashSet<String> currentTopics = new HashSet<String>();
		Map<String, List<PartitionInfo> > topics;

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test-consumer-group");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		topics = consumer.listTopics();
		Collection<List<PartitionInfo>> pinfo = topics.values();
		Iterator<List<PartitionInfo>> ipinfo = pinfo.iterator();
		List<PartitionInfo> lpinfo;
		PartitionInfo pi;
		
		while(ipinfo.hasNext()){
			lpinfo = ipinfo.next();
			Iterator<PartitionInfo> ip =  lpinfo.iterator();
			
			while(ip.hasNext()){
				pi = ip.next();
				currentTopics.add(pi.topic());
			}
		}
		consumer.close();
		
		return currentTopics;
	}

	
	private void printToFile(String text){
		try {
			printWriter.println(text);
		} catch (Exception ex) {
			printWriter.flush();
			printWriter.close();
		}	

	}
	
	private void load(){
//		if(Keys.mode.compareTo("2") == 0){
//			try {
//				printWriter = new PrintWriter(Keys.fileName, "UTF-8");
//			} catch (FileNotFoundException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} catch (UnsupportedEncodingException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}

		
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Keys.brokerUrl);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		properties.put(ProducerConfig.ACKS_CONFIG, "0");
		properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "50");
		properties.put(ProducerConfig.LINGER_MS_CONFIG, "50");
		properties.put(ProducerConfig.RETRIES_CONFIG, "1");
		properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "50");
		properties.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, "50");
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, "productorAGB");

		producer = new KafkaProducer<>(properties);
	}
}
