package costumdeserializer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class OrderConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer",OrderDeserializer.class.getName());
        properties.setProperty("group.id","consumerOrder");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String,Order> consumer = new KafkaConsumer<String, Order>(properties);
        Map<TopicPartition ,OffsetAndMetadata> map = new HashMap<>();
        class RebalancedHandler implements ConsumerRebalanceListener{

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                consumer.commitSync(map);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {

            }
        }
        //consumer.subscribe(Collections.singletonList("OrderTopicCS"));
        consumer.subscribe(Collections.singletonList("OrderTopicCScmd") , new RebalancedHandler());
        try {
            while (true) {
                int count = 0;
                ConsumerRecords<String, Order> record = consumer.poll(Duration.ofSeconds(20));
                for (ConsumerRecord<String, Order> order : record) {
                    System.out.println("Customer name " + order.key());
                    Order o = order.value();
                    System.out.println("Customer product " + o.getProduct());
                    System.out.println("Order quantity " + o.getQuantity());
                    System.out.println("partition " + record.partitions());
                    map.put(new TopicPartition(order.topic(),
                            order.partition()) , new OffsetAndMetadata(order.offset()));
                    if(count%10==0){
                        consumer.commitAsync(map,
                                new OffsetCommitCallback() {
                            @Override
                            public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                                if(e != null){
                                    System.out.println("Error occurs offset : " + map);
                                }
                            }
                        });
                    }
                }
            }
        }finally {
            consumer.close();
        }

    }
}
