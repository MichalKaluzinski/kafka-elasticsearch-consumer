package com.michalkaluzinski.kafka.elasticsearch.consumer;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class ElasticSearchConsumer {

    private Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    public static void main(String[] args) throws IOException {
	new ElasticSearchConsumer().run();
    }

    public void run() throws IOException {
	RestHighLevelClient client = createClient();
	KafkaConsumer<String, String> consumer = createKafkaConsumer("twitter_tweets");
	while (true) {
	    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

	    logger.info("Received: {}", records.count());
	    for (ConsumerRecord<String, String> record : records) {
		String id = extractIdFromTweet(record.value());
		IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id);
		indexRequest.source(record.value(), XContentType.JSON);

		IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

		logger.info(indexResponse.getId());

		try {
		    Thread.sleep(10);
		} catch (InterruptedException e) {
		    // TODO Auto-generated catch block
		    e.printStackTrace();
		}
		logger.info("Commiting offsets...");
		consumer.commitSync();
	    }
	    logger.info("Offsets commited");
	    try {
		Thread.sleep(1000);
	    } catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	}
	// client.close();
    }

    private String extractIdFromTweet(String tweetJson) {
	return JsonParser.parseString(tweetJson).getAsJsonObject().get("id").getAsString();
    }

    private RestHighLevelClient createClient() {

	final Properties elasticsearchProperties = readAndLoadElasticSearchProperties();
	String hostname = elasticsearchProperties.getProperty("hostname");
	String username = elasticsearchProperties.getProperty("username");
	String password = elasticsearchProperties.getProperty("password");

	final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
	credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

	RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
		.setHttpClientConfigCallback(new HttpClientConfigCallback() {

		    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
			return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
		    }
		});

	return new RestHighLevelClient(builder);
    }

    private Properties readAndLoadElasticSearchProperties() {
	try (InputStream inputStream = this.getClass().getClassLoader()
		.getResourceAsStream("elasticsearch.properties")) {
	    Properties elastichSearchProperties = new Properties();
	    elastichSearchProperties = new Properties();
	    elastichSearchProperties.load(inputStream);
	    return elastichSearchProperties;
	} catch (IOException e1) {
	    throw new RuntimeException();
	}
    }

    private KafkaConsumer<String, String> createKafkaConsumer(String topic) {
	String bootstrapServers = "127.0.0.1:9092";
	String groupId = "my-fifth-application";

	Properties properties = new Properties();
	properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
	properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable autocommit offset
	properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
	
	// create consumer
	KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
	kafkaConsumer.subscribe(Arrays.asList(topic));

	return kafkaConsumer;
    }

}
