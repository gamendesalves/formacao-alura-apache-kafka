package br.com.estudos.ecommerce.dispatcher;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import br.com.estudos.ecommerce.CorrelationId;
import br.com.estudos.ecommerce.Message;

public class KafkaDispatcher<T> implements Closeable {

	private final KafkaProducer<String, Message<T>> producer;

	public KafkaDispatcher() {
		this.producer = new KafkaProducer<String, Message<T>>(properties());
	}

	public void send(String topic, String key, CorrelationId id, T payload)
			throws InterruptedException, ExecutionException {
		Future<RecordMetadata> future = sendAsync(topic, key, id, payload);
		future.get();
	}

	public Future<RecordMetadata> sendAsync(String topic, String key, CorrelationId id, T payload)
			throws InterruptedException, ExecutionException {
		Message<T> value = new Message<T>(id.continueWith("_" + topic), payload);
		ProducerRecord<String, Message<T>> record = new ProducerRecord<String, Message<T>>(topic, key, value);

		Callback callback = (data, ex) -> {
			if (ex != null) {
				ex.printStackTrace();
				return;
			}
			System.out.println("Sucesso enviando " + data.topic() + " ::partition " + data.partition() + "::offset "
					+ data.offset() + "::timestamp " + data.timestamp());
		};
		
		return producer.send(record, callback);
	}

	private Properties properties() {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // Importante configuração para reliability (Garante
																	// que a mensagem seja replicada nos brokers)

		// Importante realizar essa configuração para realizar a leitura de uma mensagem
		// e já avisar o kafka em seguida
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
		return properties;
	}

	@Override
	public void close() {
		this.producer.close();
	}
}
