/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package kafkaspark;



import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

public class MyKafkaProducer {

    private static KafkaProducer<Integer, String> producer = null;
    private MyKafkaProducer() {
    }

    public MyKafkaProducer(Properties props) {
		// TODO Auto-generated constructor stub
	}

	public static KafkaProducer<Integer, String> getProducer() {
        if (producer == null) {
            Properties props = new Properties();
            props.put("bootstrap.servers", "10.0.0.26:9092");
            props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer(props);

        }
        return producer;
    }
}