import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.IntegerDeserializer");
        properties.setProperty("group.id","consumerOrder");

        KafkaConsumer<String,Integer> consumer = new KafkaConsumer<String, Integer>(properties);
        consumer.subscribe(Collections.singletonList("OrderTopic"));
        ConsumerRecords<String , Integer> record = consumer.poll(Duration.ofSeconds(20));
        for(ConsumerRecord<String,Integer> order : record){
            System.out.println("Order name" + order.key());
            System.out.println("Order count " + order.value());
        }

    }
}
