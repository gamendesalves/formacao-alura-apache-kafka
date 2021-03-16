package br.com.estudos.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import br.com.estudos.ecommerce.dispatcher.KafkaDispatcher;

public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		try (KafkaDispatcher<Order> dispatcherOrder = new KafkaDispatcher<Order>()) {

			String email = Math.random() + "@email.com";
			for (int i = 0; i < 10; i++) {

				String orderId = UUID.randomUUID().toString();
				BigDecimal amount = new BigDecimal(Math.random() * 5000 + 1);
				CorrelationId id = new CorrelationId(NewOrderMain.class.getSimpleName());

				// Email como chave será para garantir que todas mensagens no topico para esse
				// email será processada em ordem.
				// Caso fosse necessario processar em paralelo, é necessario passar uma key
				// random a cada produto
				Order order = new Order(orderId, amount, email);
				dispatcherOrder.send("ECOMMERCE_NEW_ORDER", email, id, order);

			}
		}
	}

}
