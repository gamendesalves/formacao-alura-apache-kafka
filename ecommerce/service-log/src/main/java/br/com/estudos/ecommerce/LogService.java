package br.com.estudos.ecommerce;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import br.com.estudos.ecommerce.consumer.KafkaService;

public class LogService {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		LogService logService = new LogService();
		Map<String, String> extraConfigs = new HashMap<>();
		extraConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		try (KafkaService<String> service = new KafkaService<String>(LogService.class.getSimpleName(),
				Pattern.compile("ECOMMERCE.*"), logService::parse, extraConfigs)) {
			service.run();
		}

	}

	private void parse(ConsumerRecord<String, Message<String>> record) {
		System.out.println("---------------------");
		System.out.println("TOPIC - " + record.topic());
		System.out.println("Key - " + record.key());
		System.out.println("Value - " + record.value());
		System.out.println("Partition - " + record.partition());
		System.out.println("OffSet - " + record.offset());
	}

}
