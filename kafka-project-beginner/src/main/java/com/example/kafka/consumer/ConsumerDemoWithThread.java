package com.example.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThread {

	public static void main(String[] args) {
		new ConsumerDemoWithThread().run();
		 
	}
	private ConsumerDemoWithThread() {
		
	}
	
	private void run() {
		Logger logger= LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
		
		 String bootstrapserver="127.0.0.1:9092";
		 String groupId="my_sixth_application";
		 String topic="first_topic";
		 
		 //latch for dealing with multiple threads
		 CountDownLatch latch=new CountDownLatch(1);
		 
		 //creating consumer runnable
		 logger.info("Creating Consumer Thread: ");
		 Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapserver, groupId, topic, latch);
		 
		 //start my thread
		 Thread myThread=new Thread(myConsumerRunnable);
		 myThread.start();
		 
		 //add shut down hook
		 Runtime.getRuntime().addShutdownHook(new Thread( ()->{
			 logger.info("Caught Shutdown Hook");
			 ((ConsumerRunnable) myConsumerRunnable).shutdown();
			 
			 try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			 
			 logger.info(" Application has exited...");
		 }
				 ));
		 
		 
		 try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("application interrupted: ",e);
		}finally {
			logger.info("Application is closing...");
		}
	}

}


 class ConsumerRunnable  implements Runnable{
 
	 private CountDownLatch latch;
	 private KafkaConsumer<String, String> consumer;
	 private Logger logger= LoggerFactory.getLogger(ConsumerRunnable.class.getName());
	 
	 public ConsumerRunnable(String bootstrapserver,String groupId,String topic,CountDownLatch latch) {
		 this.latch=latch;
//		 this.bootstrapserver=bootstrapserver;
//		 this.groupId=groupId;
//		 this.topic=topic;
		 
		//consumer configs
		 Properties properties=new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		 
			//create a consumer
		    consumer= new KafkaConsumer<String, String>(properties);
			
			//subscribe consumer to our topic(s)
			consumer.subscribe(Arrays.asList(topic));
	 }
	 
	@Override
	public void run() {

		//poll for new data
		try {
		while(true) {
			ConsumerRecords<String, String> records=consumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord<String, String> record: records) {
				logger.info("Key: "+record.key()+"\n"+
							"Value: "+record.value()+"\n"+
							"Partition: "+record.partition()+"\n"+
							"Offset: "+record.offset());
				
			}
		}
		}catch(WakeupException exception) {
			logger.info("Receive a shutdown signal!");
		}finally {
			consumer.close();
			
			//tell the main code that we are done with the consumer
			latch.countDown();
		}
		
	}
	
	public void shutdown() {
		//wakeup() method is a special method to interrupt consumer.poll()
		//it will throw wakeup exception
		consumer.wakeup();
	}
	
}
