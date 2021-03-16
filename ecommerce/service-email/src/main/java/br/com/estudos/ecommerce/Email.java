package br.com.estudos.ecommerce;

public class Email {

	private final String subject, body;

	public Email(String subject, String body) {
		super();
		this.subject = subject;
		this.body = body;
	}

	public String getSubject() {
		return subject;
	}

	public String getBody() {
		return body;
	}

	@Override
	public String toString() {
		return "Email [subject=" + subject + ", body=" + body + "]";
	}

}
