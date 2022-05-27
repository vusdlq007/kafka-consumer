import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerApplication {
    public static void main(String[] args) {

        try {
            // set kafka properties
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");  // kafka cluster
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());// KEY_SERIALIZER
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // VALUE_SERIALIZER
            props.put(ConsumerConfig.GROUP_ID_CONFIG,"testgroup");
            //props.setProperty("enable.auto.commit", "false");

            // init KafkaConsumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

            consumer.subscribe(Arrays.asList("testTopic")); // topic list
            final int minBatchSize = 200;

            List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));  // polling interval
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.value());
                    buffer.add(record);
                }
                if (buffer.size() >= minBatchSize) {
                    System.out.println(buffer);
                    consumer.commitSync();
                    buffer.clear();
                }
            }
        } catch (Exception e) {
            System.out.println(e);
        }

    }
}
