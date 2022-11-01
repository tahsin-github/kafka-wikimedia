package WikimediaChanges;

import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws IOException, InterruptedException {
        // Read the Kafka Server's ip address from the properties file
        Properties kafkaServerIPProperties = new Properties();
        InputStream is = new FileInputStream("kafka-producer/src/main/java/properties/KafkaServer.properties");
        kafkaServerIPProperties.load(is);

        // Create Kafka Producer Properties
        Properties KafkaProperties = new Properties();

        KafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerIPProperties.getProperty("bootstrap.servers"));
        KafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "100");
        KafkaProperties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        KafkaProperties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(KafkaProperties);

        // Create a message/producer record

        final String topic = "wikimedia.recentchange";

        WikimediaChangeHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();


        // Start the producer in another thread
        eventSource.start();

        TimeUnit.MINUTES.sleep(1000);
    }
}
