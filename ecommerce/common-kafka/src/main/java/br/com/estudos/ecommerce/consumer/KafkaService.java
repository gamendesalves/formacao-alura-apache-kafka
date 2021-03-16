package br.com.estudos.ecommerce.consumer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import br.com.estudos.ecommerce.Message;
import br.com.estudos.ecommerce.dispatcher.GsonSerializer;
import br.com.estudos.ecommerce.dispatcher.KafkaDispatcher;

public class KafkaService<T> implements Closeable {

	private final KafkaConsumer<String, Message<T>> consumer;
	private final ConsumerFunction<Message<T>> parse;

	public KafkaService(String groupId, Pattern topic, ConsumerFunction<Message<T>> parse,
			Map<String, String> properties) {
		this(parse, groupId, properties);
		this.consumer.subscribe(topic);
	}

	public KafkaService(String groupId, String topic, ConsumerFunction<Message<T>> parse,
			Map<String, String> properties) {
		this(parse, groupId, properties);
		this.consumer.subscribe(Collections.singletonList(topic));
	}

	private KafkaService(ConsumerFunction<Message<T>> parse, String groupId, Map<String, String> properties) {
		this.parse = parse;
		this.consumer = new KafkaConsumer<String, Message<T>>(this.getProperties(groupId, properties));
	}

	public void run() throws InterruptedException, ExecutionException {

		try (KafkaDispatcher<byte[]> deadLetter = new KafkaDispatcher<byte[]>()) {
			// Fica ouvindo se tem mensagens
			while (true) {
				ConsumerRecords<String, Message<T>> records = consumer.poll(Duration.ofMillis(100));
				if (!records.isEmpty()) {
					System.out.println("Encontrei " + records.count() + " registros");
					for (ConsumerRecord<String, Message<T>> record : records) {
						try {
							parse.consume(record);
						} catch (Exception e) {
							e.printStackTrace();
							// Envia mensagem informando que ouve erros - DeadLetter
                            Message<T> message = record.value();
                            deadLetter.send("ECOMMERCE_DEADLETTER", message.getId().toString(),
                                    message.getId().continueWith("DeadLetter"),
                                    new GsonSerializer().serialize("", message));
						}
					}
				}
			}
		}
	}

	private Properties getProperties(String groupId, Map<String, String> overrideProperties) {
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());

		// Definindo grupo para consumir mensagens
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		properties.putAll(overrideProperties);

		return properties;
	}

	@Override
	public void close() {
		consumer.close();
	}

}
