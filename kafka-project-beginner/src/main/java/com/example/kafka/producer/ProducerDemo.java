package com.example.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	public static void main(String[] args) {
		
		String bootstrapServer="127.0.0.1:9092";
		
		//producer properties
		Properties properties=new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//create a producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		//create a record
		ProducerRecord<String, String> record= new ProducerRecord<String, String>("first_topic", "hello world");
		
		//send data->asynchronous
		producer.send(record);
		
		//flush data
		producer.flush();
		
		//producer flush and close
		producer.close();

	}

}
