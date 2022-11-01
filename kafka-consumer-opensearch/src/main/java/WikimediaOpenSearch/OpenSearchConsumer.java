package WikimediaOpenSearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient() throws IOException {

        // Read the Kafka Server's ip address from the properties file
        Properties openSearchServer = new Properties();
        InputStream is = new FileInputStream("kafka-consumer-opensearch/src/main/java/properties/servers.properties");
        openSearchServer.load(is);
        String connString = openSearchServer.getProperty("wikimedia.servers");


        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }



    public static void main(String[] args) throws IOException {

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getName());

        // create an OpenSearch Client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // Create Kafka Client
        KafkaConsumer<String, String> consumer = createKafkaConsumer();
        // Subscribe the consumer to a topic
        final String topic = "wikimedia.recentchange";
        consumer.subscribe(Arrays.asList(topic));

        try(openSearchClient; consumer){
            boolean indexExist = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);



            if(!indexExist){
                try(openSearchClient){
                    CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                    openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                    log.info("The wikimedia Index has been created");
                }
            }
            else {
                log.info("The wikimedia Index is already existed.");
            }

            while (true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));

                int recordCount = records.count();
                log.info("Received " + recordCount + " records.");

                BulkRequest bulkRequest = new BulkRequest();

//                int i = 0;

                for (ConsumerRecord<String, String> record : records){
                    try{

                        String id = extractId(record.value());

                        log.info(" The id extracted from the json is : " + id);

                        // send the data to the opensearch
                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id);

                        bulkRequest.add(indexRequest);

//                        IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

//                        i+=1;

//                        log.info("Document inserted : " + i + " " + response.getId());
                    }
                    catch (Exception e){
                        e.printStackTrace();
                    }
                }

                if(bulkRequest.numberOfActions() > 0){
                    BulkResponse bulkResponse =  openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted " + bulkResponse.getItems().length + " records.");

                    try {
                        Thread.sleep(1000);
                    }
                    catch (InterruptedException e){
                        e.printStackTrace();
                    }

                    consumer.commitSync();
                    log.info("Offset has been committed");
                }


            }

        }


        // Main Code Logic

        // Close things
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() throws IOException {
        final String groupId = "consumer-opensearch.one";


        // Read the Kafka Server's ip address from the properties file
        Properties kafkaServerIPProperties = new Properties();
        InputStream is = new FileInputStream("kafka-consumer-opensearch/src/main/java/properties/servers.properties");
        kafkaServerIPProperties.load(is);

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServerIPProperties.getProperty("bootstrap.servers"));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");


        // Create the Consumer
        return new KafkaConsumer<String, String>(properties);


    }

    private static String extractId(String json){
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }
}
