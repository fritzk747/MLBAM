import java.sql.*;
import java.util.ArrayList;

public class SendDataToSQL{
	public SendDataToSQL(){
	
	}
	
	public void bulkInsert(String dataFile, String host, String port, String user, String password, String database, String tableName, String fieldTerminator, String rowTerminator, StringBuilder errorLog){
		Statement stmt = null;
		String query = "BULK INSERT " + tableName + " FROM '" + dataFile + "' WITH (FIELDTERMINATOR ='" + fieldTerminator + "',ROWTERMINATOR ='" + rowTerminator + "');";
		Connection connection = null;
		
		try
		{
			// the sql server driver string
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			
			// the sql server url
			String url = "jdbc:sqlserver://" + host + ":" + port + ";DatabaseName=" + database;
			
			connection = DriverManager.getConnection(url,user,password);
			
			// now do whatever you want to do with the connection
			// ...
			stmt = connection.createStatement();
			stmt.addBatch(query);
			stmt.executeBatch();
			if (stmt != null) { stmt.close(); }
		}
		catch (ClassNotFoundException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			System.exit(2);
		}
		
	}
	
	public void bulkInsert(String dataFile, String host, String port, String user, String password, String database, String tableName, StringBuilder errorLog){
		bulkInsert(dataFile, host, port, user, password, database, tableName, ",", "\\n", errorLog);
	}
	
	public void executeQuery(String host, String port, String user, String password, String database, String query, StringBuilder errorLog){
		Statement stmt = null;
		Connection connection = null;
		
		try
		{
			// the sql server driver string
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			
			// the sql server url
			String url = "jdbc:sqlserver://" + host + ":" + port + ";DatabaseName=" + database;
			
			connection = DriverManager.getConnection(url,user,password);
			
			// now do whatever you want to do with the connection
			// ...
			stmt = connection.createStatement();
			stmt.addBatch(query);
			stmt.executeBatch();
			if (stmt != null) { stmt.close(); }
		}
		catch (ClassNotFoundException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			System.exit(2);
		}
	}
			
	public ResultSet getQuery(String host, String port, String user, String password, String database, String query, StringBuilder errorLog){
		Statement stmt = null;
		Connection connection = null;
		ResultSet rs = null;
		
		String url = "jdbc:sqlserver://" + host + ":" + port + ";DatabaseName=" + database;
		
		try
		{
			// the sql server driver string
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

			connection = DriverManager.getConnection(url,user,password);
			
			// now do whatever you want to do with the connection
			// ...
			stmt = connection.createStatement();
			rs = stmt.executeQuery(query);
			//if (stmt != null) { stmt.close(); }
		}
		catch (ClassNotFoundException e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			System.exit(2);
		}
		
		return rs;
	}
}