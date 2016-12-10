package com.master2016;


import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaTwitterProducer {

	public KafkaTwitterProducer() {

	}

	
	public void fromFile(){
		
	}
	
	public void fromTwitter() throws InterruptedException {

		Properties properties = new Properties();
		properties.put("metadata.broker.list", Keys.brokerUrl);
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		properties.put("client.id", "productorAGB");
		ProducerConfig producerConfig = new ProducerConfig(properties);
		kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
				producerConfig);

		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(100000);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		endpoint.trackTerms(Lists.newArrayList("twitter"));

		String consumerKey = Keys.apiKey;
		String consumerSecret = Keys.apiSecret;
		String accessToken = Keys.tokenValue;
		String accessTokenSecret = Keys.tokenSecret;

		Authentication auth = new OAuth1(consumerKey, consumerSecret, accessToken, accessTokenSecret);

		Client client = new ClientBuilder().hosts(Constants.STREAM_HOST).endpoint(endpoint).authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();

		client.connect();

		for (int msgRead = 0; msgRead < 10; msgRead++) {
			KeyedMessage<String, String> message = null;
			try {
				System.out.println(queue.take());
				message = new KeyedMessage<String, String>("incoming", queue.take());
			} catch (InterruptedException e) {
				// e.printStackTrace();
				System.out.println("Stream ended");
			}
			producer.send(message);
		}
		producer.close();
		client.stop();
	}

}
