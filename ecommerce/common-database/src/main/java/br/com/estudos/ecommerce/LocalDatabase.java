package br.com.estudos.ecommerce;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LocalDatabase {

	private Connection connection;

	public LocalDatabase(String database) throws SQLException {
		String url = "jdbc:sqlite:target/" + database + ".db";
		this.connection = DriverManager.getConnection(url);
	}

	public void createIfNotExists(String sql) {
		try {
			connection.createStatement().execute(sql);
		} catch (SQLException ex) {
			ex.printStackTrace();
		}
	}

	public boolean update(String statement, String... params) throws SQLException {
		return prepare(statement, params).execute();
	}

	public ResultSet query(String statement, String... params) throws SQLException {
		return prepare(statement, params).executeQuery();
	}

	private PreparedStatement prepare(String statement, String... params) throws SQLException {
		PreparedStatement preparedStatement = this.connection.prepareStatement(statement);
		for (int i = 0; i < params.length; i++) {
			preparedStatement.setString(i + 1, params[i]);
		}
		return preparedStatement;
	}
	
	public void close() throws SQLException {
		connection.close();
	}
}
