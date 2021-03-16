package br.com.estudos.ecommerce;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.estudos.ecommerce.consumer.ConsumerService;
import br.com.estudos.ecommerce.consumer.ServiceRunner;

public class CreateUserService implements ConsumerService<Order> {

	private final LocalDatabase database;

	public CreateUserService() throws SQLException {
		this.database = new LocalDatabase("users_database");
		this.database.createIfNotExists("create table Users (uuid varchar(200) primary key, email varchar(200))");
	}

	public static void main(String[] args) {
		new ServiceRunner<>(CreateUserService::new).start(1);
	}

	@Override
	public String getTopic() {
		return "ECOMMERCE_NEW_ORDER";
	}

	@Override
	public String getConsumerGroup() {
		return CreateUserService.class.getSimpleName();
	}

	@Override
	public void parse(ConsumerRecord<String, Message<Order>> record)
			throws InterruptedException, ExecutionException, SQLException {
		System.out.println("---------------------");
		System.out.println("Processing new order, checking for new user");
		System.out.println("Value - " + record.value());
		Order order = record.value().getPayload();

		if (this.isNewUser(order.getEmail())) {
			this.insertNewUser(order);
		}
	}

	private void insertNewUser(Order order) throws SQLException {
		String uuid = UUID.randomUUID().toString();
		this.database.update("insert into Users (uuid, email) values (?, ?)", uuid, order.getEmail());
		System.out.println("User added - " + order);
	}

	private boolean isNewUser(String email) throws SQLException {
		ResultSet results = this.database.query("select uuid from Users where email = ? limit 1", email);
		return !results.next();
	}

}
