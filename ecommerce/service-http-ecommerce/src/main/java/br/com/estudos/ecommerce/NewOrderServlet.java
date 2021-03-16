package br.com.estudos.ecommerce;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import br.com.estudos.ecommerce.dispatcher.KafkaDispatcher;

public class NewOrderServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	private final KafkaDispatcher<Order> dispatcherOrder = new KafkaDispatcher<Order>();

	@Override
	public void destroy() {
		super.destroy();
		dispatcherOrder.close();

	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		try {
			String email = req.getParameter("email");
			// String orderId = UUID.randomUUID().toString();
			BigDecimal amount = new BigDecimal(req.getParameter("amount"));
			String orderId = req.getParameter("uuid");

			// Email como chave será para garantir que todas mensagens no topico para esse
			// email será processada em ordem.
			// Caso fosse necessario processar em paralelo, é necessario passar uma key
			// random a cada produto
			Order order = new Order(orderId, amount, email);

			try (OrdersDatabase database = new OrdersDatabase()) {
				if (!database.saveNewOrder(order)) {

					dispatcherOrder.send("ECOMMERCE_NEW_ORDER", email,
							new CorrelationId(NewOrderServlet.class.getSimpleName()), order);
					System.out.println("New Order send sucessfully");
					resp.setStatus(200);
					resp.getWriter().println("New order send");
				} else {
					System.out.println("Old order received");
					resp.setStatus(200);
					resp.getWriter().println("Old order received");
				}
			}
		} catch (InterruptedException | ExecutionException | SQLException e) {
			throw new ServletException(e);
		}
	}

}
