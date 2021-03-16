package br.com.estudos.ecommerce.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.estudos.ecommerce.Message;

public interface ConsumerService<T> {
	
	void parse(ConsumerRecord<String, Message<T>> record) throws Exception;

	String getTopic();

	String getConsumerGroup();
}
