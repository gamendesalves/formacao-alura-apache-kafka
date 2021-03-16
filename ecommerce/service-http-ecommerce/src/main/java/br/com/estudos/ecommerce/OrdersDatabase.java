package br.com.estudos.ecommerce;

import java.io.Closeable;
import java.sql.ResultSet;
import java.sql.SQLException;

public class OrdersDatabase implements Closeable {

	private final LocalDatabase database;

	public OrdersDatabase() throws SQLException {
		this.database = new LocalDatabase("orders_database");
		this.database.createIfNotExists("create table Orders (uuid varchar(200) primary key)");
	}

	public boolean saveNewOrder(Order order) throws SQLException {
		if (this.wasProcessed(order)) {
			return false;
		}
		this.database.update("insert into Orders (uuid) values (?)", order.getOrderId());
		return true;
	}

	private boolean wasProcessed(Order order) throws SQLException {
		ResultSet results = this.database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());
		return results.next();
	}

	@Override
	public void close()  {
		try {
			this.database.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
