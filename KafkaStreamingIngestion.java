package com.upgrad.CapstoneProject;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class KafkaStreamingIngestion {

	//The below method is used to get the current time stamp so that it can be appended to GroupId to make it unique
	public static String getCurrentTimeUsingDate() {
		Date date = new Date();
		DateFormat dateFormat = new SimpleDateFormat("dd-MM-yy HH:mm:ss");
		return dateFormat.format(date);
	}

	public static String GROUP_ID = "DCkafkaspark"+ getCurrentTimeUsingDate();

	public static void main(String[] args) throws Exception {

		System.out.println("Using group ID : " + GROUP_ID + "  for current Kafka stream.");

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		//Setting Spark Conf
		SparkConf sparkConf = new SparkConf().setAppName("FraudAnalysis").setMaster("local");

		//Setting streaming Context
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
		

		/*
		 * Initializing the static variable in NoSQLDBConAndUpdates class
		 * The NoSQLDBConAndUpdates class is the HBase connection class
		 * The HBase master and zookeeper are dependent on EC2 public IP, which changes with each time EC2 instance is started.
		 * Hence that variable is taken from the user and passed to the HBase connection class
		 * We make use of a static variable ec2PublicIP to do this.
		 * Another variable is zipCodePath which is again a user input.This is passed to Distance Utility class
		 */
		NoSQLDBConAndUpdates.ec2PublicIP = args[0];
		DistanceUtility.zipCodePath = args[1];

		//Setting parameters for Kafka stream
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "100.24.223.181:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", GROUP_ID);
		kafkaParams.put("auto.offset.reset", "earliest");
		kafkaParams.put("enable.auto.commit", true);

		//Subscribing to Kafka topic
		Collection<String> topics = Arrays.asList("transactions-topic-verified");

		//Consuming input Kafka Stream into Spark DStreams
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		//Applying map transformation on input stream. We are interested only in the value part
		JavaDStream<String> jds = stream.map(x -> x.value());

		//We want to filter only those records which are of the format that we need.
		JavaDStream<String> jds_filtered = jds.filter(new Function<String,Boolean>(){

			private static final long serialVersionUID = 1L;

			public Boolean call(String row) throws Exception {
				return row.startsWith("{\"card_id\":");
			}		
		});

		/* Below code can be uncommented if you want to see the record count. 
		 * For current topic , it came as 6075 records
		System.out.println("Printing the count of filtered records now");
		jds_filtered.foreachRDD(x -> System.out.println(x.count()));
		 * 
		 */

		//Applying map transformation on filtered stream and invoking the FraudDetection class constructor
		JavaDStream<FraudDetection> jds_final = jds_filtered.map(x -> new FraudDetection(x));

		//Calling the updateHBaseTable method in FraudDetection class
		jds_final.foreachRDD(new VoidFunction<JavaRDD<FraudDetection>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<FraudDetection> rdd)  {
				rdd.foreach(x -> x.updateHBaseTable(x));
			}

		});


		//Run the streaming job
		jssc.start();
		jssc.awaitTermination();


	}
}
