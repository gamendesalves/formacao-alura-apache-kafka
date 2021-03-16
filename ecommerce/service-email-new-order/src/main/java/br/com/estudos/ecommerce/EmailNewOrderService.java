package br.com.estudos.ecommerce;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.estudos.ecommerce.consumer.ConsumerService;
import br.com.estudos.ecommerce.consumer.ServiceRunner;
import br.com.estudos.ecommerce.dispatcher.KafkaDispatcher;

public class EmailNewOrderService implements ConsumerService<Order> {

	private final KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<Email>();

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		new ServiceRunner<>(EmailNewOrderService::new).start(1);
	}

	@Override
	public String getTopic() {
		return "ECOMMERCE_NEW_ORDER";
	}

	@Override
	public String getConsumerGroup() {
		return EmailNewOrderService.class.getSimpleName();
	}

	@Override
	public void parse(ConsumerRecord<String, Message<Order>> record) throws InterruptedException, ExecutionException {
		System.out.println("------------------------------------------");
		System.out.println("Processing new order, preparing email");
		System.out.println(record.value());

		Message<Order> message = record.value();

		Email emailCode = new Email("E-mail Ecommerce", "Obrigado! Nós estamos processando sua order");
		emailDispatcher.send("ECOMMERCE_SEND_EMAIL", message.getPayload().getEmail(),
				message.getId().continueWith(EmailNewOrderService.class.getSimpleName()), emailCode);
	}

}
