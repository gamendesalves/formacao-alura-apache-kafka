package br.com.estudos.ecommerce;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.estudos.ecommerce.consumer.ConsumerService;
import br.com.estudos.ecommerce.consumer.ServiceRunner;
import br.com.estudos.ecommerce.dispatcher.KafkaDispatcher;

public class FraudDetectorService implements ConsumerService<Order> {

	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

	private final LocalDatabase database;

	public FraudDetectorService() throws SQLException {
		this.database = new LocalDatabase("frauds_database");
		this.database.createIfNotExists("create table Orders (uuid varchar(200) primary key, is_fraud boolean)");
	}

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		new ServiceRunner<>(FraudDetectorService::new).start(1);
	}

	@Override
	public String getTopic() {
		return "ECOMMERCE_NEW_ORDER";
	}

	@Override
	public String getConsumerGroup() {
		return FraudDetectorService.class.getSimpleName();
	}

	@Override
	public void parse(ConsumerRecord<String, Message<Order>> record)
			throws ExecutionException, InterruptedException, SQLException {
		System.out.println("------------------------------------------");
		System.out.println("Processing new order, checking for fraud");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());

		Order order = record.value().getPayload();
		if (this.wasProcessed(order)) {
			System.out.println("Order " + order.getOrderId() + " was already processed");
			return;
		}

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// ignoring
			e.printStackTrace();
		}

		if (this.isFraud(order)) {
			this.database.update("insert into Orders (uuid, is_fraud) values (?, true)", order.getOrderId());
			// pretending that the fraud happens when the amount is >= 4500
			System.out.println("Order is a fraud!!!!!" + order);
			orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(),
					record.value().getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
		} else {
			System.out.println("Approved: " + order);
			orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(),
					record.value().getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
		}

	}

	private boolean wasProcessed(Order order) throws SQLException {
		ResultSet results = this.database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());
		return results.next();
	}

	private boolean isFraud(Order order) {
		return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
	}

}
