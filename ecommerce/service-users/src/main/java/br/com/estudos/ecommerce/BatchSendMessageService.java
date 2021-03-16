package br.com.estudos.ecommerce;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.estudos.ecommerce.consumer.KafkaService;
import br.com.estudos.ecommerce.dispatcher.KafkaDispatcher;

public class BatchSendMessageService {

	private Connection connection;

	private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<User>();

	public BatchSendMessageService() {
		try {
			String url = "jdbc:sqlite:target/users_database.db";
			this.connection = DriverManager.getConnection(url);
			connection.createStatement()
					.execute("create table Users (uuid varchar(200) primary key, email varchar(200))");
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
	}

	public static void main(String[] args) throws SQLException, InterruptedException, ExecutionException {
		BatchSendMessageService batchService = new BatchSendMessageService();

		try (KafkaService<String> service = new KafkaService<String>(BatchSendMessageService.class.getSimpleName(),
				"ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS", batchService::parse, new HashMap<>())) {
			service.run();
		}
	}

	public void parse(ConsumerRecord<String, Message<String>> record)
			throws InterruptedException, ExecutionException, SQLException {
		System.out.println("---------------------");
		System.out.println("Processing new batch");
		System.out.println("Topic " + record.value().getPayload());

		for (User user : getAllUsers()) {
			userDispatcher.sendAsync(record.value().getPayload(), user.getUuid(),
					record.value().getId().continueWith(BatchSendMessageService.class.getSimpleName()), user);
		}
	}

	private List<User> getAllUsers() throws SQLException {
		PreparedStatement statement = connection.prepareStatement("select uuid from Users");
		ResultSet results = statement.executeQuery();

		List<User> users = new ArrayList<>();
		while (results.next()) {
			users.add(new User(results.getString(1)));
		}

		return users;
	}

}
