package com.example.kafka.elasticsearch;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class ElasticSearchConsumer {

	private static String index="twitter";
	private static String index_type="tweets";
	//private static String jsonString="{\"foo\":\"bar\"}";
	private static String topic="twitter_tweets";
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException {
		Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
		
		RestHighLevelClient client = createClient();
		
		KafkaConsumer<String,String> consumer = createConsumer(topic);
		
		while(true) {
			ConsumerRecords<String, String> records=consumer.poll(Duration.ofMillis(100));
			
			Integer recCount=records.count();
			logger.info("Received "+recCount+" records");
			
			BulkRequest bulkRequest= new BulkRequest();
			
			for(ConsumerRecord<String, String> record: records) {
				
				try {
				String id = extractIdFromTweet(record.value());
				//data will be added to elastic search
				IndexRequest request = new IndexRequest(index,
									index_type,
									id) //id is used to make consumer idempotent
									.source(record.value(),XContentType.JSON);
				bulkRequest.add(request);
				
//				IndexResponse response = client.index(request, RequestOptions.DEFAULT);
//				//String id= response.getId();
//				logger.info(response.getId());
				}catch(NullPointerException exception) {
					
					logger.warn("Skipping bad request..."+ record.value());
				}
				
			}
			
			if(recCount>0) {
				BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
				logger.info("Committing offsets...");
				consumer.commitSync();
				logger.info("Offsets have been committed...");

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		//client.close();
		
	}
	
	public static KafkaConsumer<String, String> createConsumer(String topic){
		
		String bootstrapserver="127.0.0.1:9092";
		String groupId="kafka-demo-elasticsearch";
		
		
		//consumer configs
		
		Properties properties=new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
		
		//create a consumer
		KafkaConsumer<String, String> consumer= new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Arrays.asList(topic));
		
		return consumer;
	}

	public static RestHighLevelClient createClient() {

		String hostname="";
		String username="";
		String password="";
		
		//don't do if using local Elastic Search
		final CredentialsProvider credentialsProvider= new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
		
		RestClientBuilder builder=  RestClient.builder(new HttpHost(hostname,443,"https"))
						      .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
											
					       return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
								}
								});
		
		RestHighLevelClient client = new RestHighLevelClient(builder);
		
		return client;
	}
	
	private static JsonParser jsonParser = new JsonParser();
	
	private static String extractIdFromTweet(String tweetJson) {
		return jsonParser.parse(tweetJson)
				  .getAsJsonObject()
				  .get("id_str")
				  .getAsString();
	}
}
