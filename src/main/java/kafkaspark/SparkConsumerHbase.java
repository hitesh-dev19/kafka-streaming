package kafkaspark;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class SparkConsumerHbase {

	public static void main(String[] args) throws InterruptedException, IOException {

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "10.0.0.26:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", true);
		// kafkaParams.put("partition.assignment.strategy", "range");
		Collection<String> topics = Arrays.asList("stream");

		SparkSession spark = SparkSession.builder().appName("Stream").getOrCreate();


		JavaSparkContext javaCon = new JavaSparkContext(spark.sparkContext());
		JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaCon, Durations.seconds(1));

		JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(javaStreamingContext,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
		
		System.out.println("got message/......");
		System.out.println("count: "+messages.inputDStream());
		messages.foreachRDD(record -> {
			
			record.foreach(row -> {
				System.out.println(row);
				KafkaProducer<Integer, String> producer = MyKafkaProducer.getProducer();
				producer.send(new ProducerRecord<>("output", 1, row + "... here my business logic will be added..."));
			});
			

		});
		javaStreamingContext.start();
		javaStreamingContext.awaitTermination();
	}
}