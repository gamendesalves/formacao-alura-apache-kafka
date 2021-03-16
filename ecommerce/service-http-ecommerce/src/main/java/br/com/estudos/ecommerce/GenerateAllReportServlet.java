package br.com.estudos.ecommerce;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import br.com.estudos.ecommerce.dispatcher.KafkaDispatcher;

public class GenerateAllReportServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	private final KafkaDispatcher<String> batchDispatcher = new KafkaDispatcher<String>();

	@Override
	public void destroy() {
		super.destroy();
		batchDispatcher.close();
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		try {

			batchDispatcher.send("ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS", 
					"ECOMMERCE_USER_GENERATE_READING_REPORT",
					new CorrelationId(
							GenerateAllReportServlet.class.getSimpleName()),
					"ECOMMERCE_USER_GENERATE_READING_REPORT");

			System.out.println("Send generate report to all users");
			resp.setStatus(200);
			resp.getWriter().println("Report requests generated");

		} catch (InterruptedException | ExecutionException e) {
			throw new ServletException(e);
		}
	}

}
