package com.example.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {

	public static void main(String[] args) {
		
		Logger logger=LoggerFactory.getLogger(ProducerDemoWithCallback.class);
		String bootstrapServer="127.0.0.1:9092";
		
		//producer properties
		Properties properties=new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//create a producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		//create a record
		ProducerRecord<String, String> record= 
						new ProducerRecord<String, String>("first_topic","hello world");
		
		//send data->asynchronous
		producer.send(record,new Callback() {
			
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				
				//execute every time record is sent successfully otherwise exception is thrown
				
				if(exception==null) {
					
					//record was successfully sent
					logger.info("received new metadata \n"+
								"Topic:"+metadata.topic()+"\n"+
								"Partition:"+metadata.partition()+"\n"+
								"Offset:"+metadata.offset()+"\n"+
								"Timestamp"+metadata.timestamp()
							);
				}else {
					
					logger.error("Error while producing:",exception);
					//exception.printStackTrace();
				}
				
			}
		});
		
		//flush data
		producer.flush();
		
		//producer flush and close
		producer.close();

	}

}
