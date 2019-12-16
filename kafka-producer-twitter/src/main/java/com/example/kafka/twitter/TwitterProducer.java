package com.example.kafka.twitter;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {

	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
	
	private String consumerKey= "CQZZs3nQUNfQ2uyMnhrmH5rX0";
	private String consumerSecret= "FefYvQOrMkLuoHxbuOzvOa26wnAYrfbXrngNsSfW8KCToCW7ns";
	private String token= "1202897455051755520-Y0Rjf0V5ZdVXbu4Sw97wUJm8Zu1jDy";
	private String secret= "jthc0EGRHOsHCUxevqfgouw06DTAIMceDkdOQzaK7cE8y";
	
	private String topic="twitter_tweets";
	List<String> terms = Lists.newArrayList("cricket");
	
	private String idempotence="true";
	private String acks="all";
	private String flight_request="5";
	private String retries=Integer.toString(Integer.MAX_VALUE);
	
	private String compressionType="snappy";
	private String linger_ms="20";
	private String batch_size=Integer.toString(32*1024);
	
	public TwitterProducer() {}
	
	
	public static void main(String[] args) {
		new TwitterProducer().run();
	}
	
	public void run() {
		logger.info("Setup");

		/** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);


		//create a twitter client
		Client client = createTwitterClient(msgQueue);
		//attempts to establish a connection
		client.connect();

		//create a kafka producer
		KafkaProducer<String, String> producer = createKafkaProducer();
		
		//add a shutdownhook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Closing the Client...");
			client.stop();
			logger.info("producer closing.....");
			producer.close();
		}));


		//loop to send data to kafka

		// on a different thread, or multiple different threads....
		while (!client.isDone()) {
			String msg=null;
			try {
				msg = msgQueue.poll(5,TimeUnit.SECONDS);
			}catch(InterruptedException exception) {
				logger.error("Interrupted...",exception);
				client.stop();
			}

			if(msg!= null) {
				logger.info(msg);
				producer.send(new ProducerRecord<String, String>(topic, null,msg),new Callback() {

					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {

						if(exception!=null) {
							logger.error("Something bad has happened",exception);
						}

					}
				});
			}
		}
		logger.info("End of application...");
	}
	
	

	public Client createTwitterClient(BlockingQueue<String> msgQueue ) {

		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

		// Optional: set up some followings and track terms

		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);


		//create a client
		ClientBuilder builder = new ClientBuilder()
				.name("Hosebird-Client-01")                              // optional: mainly for the logs
				.hosts(hosebirdHosts)
				.authentication(hosebirdAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue)); 

		Client hosebirdClient = builder.build();
		return hosebirdClient;
	}
	
	public KafkaProducer createKafkaProducer() {
		
		String bootstrapServer="127.0.0.1:9092";

		//producer properties
		Properties properties=new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		//safe producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotence);
		properties.setProperty(ProducerConfig.ACKS_CONFIG, acks);
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, flight_request);
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, retries);
		
		//high throughput producer
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, linger_ms);
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, batch_size);
		
		//create a producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		return producer;
	}
}
