package kafkaspark;


import java.io.IOException;
import java.nio.charset.Charset;
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
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

public class SparkConsumer {

    public static void main(String[] args) throws InterruptedException, IOException {
    	

    	Logger.getLogger("org").setLevel(Level.OFF);
    	Logger.getLogger("akka").setLevel(Level.OFF);
    	Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", true);
       // kafkaParams.put("partition.assignment.strategy", "range");
        Collection<String> topics = Arrays.asList("test");

        SparkConf sparkConf = new SparkConf();
       // sparkConf.setMaster("local[*]");
        //sparkConf.setAppName("sparkstreaming");
       // sparkConf.set("spark.cassandra.connection.host", "127.0.0.1");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(10));

        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.
        		createDirectStream(streamingContext, LocationStrategies.PreferConsistent(),
        				ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));
        
       // new HBaseConnection().HBase();
        messages.foreachRDD(record->{
        	record.foreach(row->{
        		System.out.println(row.value());
        		
        			/*logic  
        			 * 
        			 * if your record have fault data, push it to error topic
        			 */
        		   KafkaProducer<Integer, String> producer = MyKafkaProducer.getProducer();
                   producer.send(new ProducerRecord<>("output", 1, row.value()+"... here my business logic will be added..."));
        		/* final HBaseConnector hBaseConnector = new HBaseConnector();
        		    hBaseConnector.connect(hbaseProps);
        		    while (partition.hasNext()) {
        		        hBaseConnector.persistMappingToHBase(partition.next());
        		    }
        		    hBaseConnector.clos exporteConnection();
                    KafkaProducer<Integer, String> producer = MyKafkaProducer.getProducer();
                    producer.send(new ProducerRecord<>("output", 1, row.value()+ table.getName()));*/

        	});
        	
        });  
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
