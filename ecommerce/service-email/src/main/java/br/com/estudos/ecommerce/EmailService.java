package br.com.estudos.ecommerce;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.estudos.ecommerce.consumer.ConsumerService;
import br.com.estudos.ecommerce.consumer.ServiceRunner;

public class EmailService implements ConsumerService<Email> {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		new ServiceRunner<>(EmailService::new).start(5);
	}

	@Override
	public String getTopic() {
		return "ECOMMERCE_SEND_EMAIL";
	}

	@Override
	public String getConsumerGroup() {
		return EmailService.class.getSimpleName();
	}

	@Override
	public void parse(ConsumerRecord<String, Message<Email>> record) {
		System.out.println("---------------------");
		System.out.println("Send e-mail");
		System.out.println("Key - " + record.key());
		System.out.println("Value - " + record.value().getPayload());
		System.out.println("Partition - " + record.partition());
		System.out.println("OffSet - " + record.offset());

		try {
			// Simulando envio do e-mail
			Thread.sleep(1000);
		} catch (InterruptedException e) {
		}

		System.out.println("E-mail sended");
	}

}
