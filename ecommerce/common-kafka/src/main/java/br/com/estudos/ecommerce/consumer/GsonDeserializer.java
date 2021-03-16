package br.com.estudos.ecommerce.consumer;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import br.com.estudos.ecommerce.Message;
import br.com.estudos.ecommerce.MessageAdapter;

@SuppressWarnings("rawtypes")
public class GsonDeserializer implements Deserializer<Message> {

	private final Gson gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageAdapter()).create();

	@Override
	public Message deserialize(String topic, byte[] bytes) {
		return gson.fromJson(new String(bytes, StandardCharsets.UTF_8), Message.class);
	}

}
