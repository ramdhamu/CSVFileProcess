package com.utility;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import com.sample.CSVFileProcess;

public class DatabaseUtil {

	public static final String CREATE_TABLE_QUERY="CREATE TABLE IF NOT EXISTS STORE_ORDER (ID INT AUTO_INCREMENT PRIMARY KEY,"+
			  "ORDER_ID VARCHAR(20) NOT NULL, ORDER_DATE DATE NOT NULL,"+
			  "SHIP_DATE DATE NOT NULL,SHIP_MODE VARCHAR(20),"+
			  "QUANTITY INT NOT NULL,  DISCOUNT DECIMAL(3,2), "+
			  "PROFIT DECIMAL(6,2) NOT NULL,PRODUCT_ID VARCHAR(20) UNIQUE NOT NULL,"+
			  "CUSTOMER_NAME VARCHAR(255) NOT NULL,CATEGORY VARCHAR(255) NOT NULL, "+
			  "CUSTOMER_ID VARCHAR(20) NOT NULL,PRODUCT_NAME VARCHAR(255));";
	
	
	/*
	 * Database connectivity
	 */
	public static Connection getConnection()throws Exception{
		try {
			String driver="com.mysql.jdbc.Driver";
			String url="jdbc:mysql://localhost:3306/store";
			String userName="root";
			String password="ROOT";
			Class.forName(driver);
			Connection connnection=DriverManager.getConnection(url, userName, password);
			return connnection;
		} catch (Exception e) {
			System.out.println(e);
			return null;
		}
		
	}
	
	
	/*
	 * Table creation process 
	 */
	public  static void createTable() throws Exception {
		Connection connection=null;
		PreparedStatement createTable=null;
		try {
			 connection= getConnection();
			 createTable=connection.prepareStatement(CREATE_TABLE_QUERY);
			 createTable.executeUpdate();
			
		} catch (Exception e) {
			System.out.println(e);
		}finally {
			createTable.close();
			connection.close();
		}
	}
	
}
