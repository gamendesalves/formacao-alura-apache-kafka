package br.com.estudos.ecommerce;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.estudos.ecommerce.consumer.ConsumerService;
import br.com.estudos.ecommerce.consumer.ServiceRunner;

public class ReadingReportService implements ConsumerService<User> {

	private final static Path SOURCE = new File("src/main/resources/report.txt").toPath();

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		new ServiceRunner<>(ReadingReportService::new).start(5);
	}

	@Override
	public String getTopic() {
		return "ECOMMERCE_USER_GENERATE_READING_REPORT";
	}

	@Override
	public String getConsumerGroup() {
		return ReadingReportService.class.getSimpleName();
	}

	@Override
	public void parse(ConsumerRecord<String, Message<User>> record) throws IOException {
		System.out.println("---------------------");
		System.out.println("Processing report for " + record.value());

		User user = record.value().getPayload();
		File target = new File(user.getReportPath());
		IO.copyTo(SOURCE, target);
		IO.append(target, "Created for " + user.getUuid());

		System.out.println("File created: " + target.getAbsolutePath());
	}

}
