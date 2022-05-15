package com.sample;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import com.utility.DatabaseUtil;

import au.com.bytecode.opencsv.CSVReader;

public class CSVFileProcess {
	
	private static final String INSRT_SQL_QUERY=" INSERT INTO store_order (ID,ORDER_ID,ORDER_DATE,SHIP_DATE,SHIP_MODE,QUANTITY,DISCOUNT,"
			+ " PROFIT,PRODUCT_ID,CUSTOMER_NAME,CATEGORY,CUSTOMER_ID,PRODUCT_NAME) VALUES "
			+ " (?,?,?,?,?,?,?,?,?,?,?,?,?)";
	
	public static void main(String[] args) throws Exception {
		
		CSVFileProcess csvFileProcess=new CSVFileProcess();
		
		//getting CSV file data to 
		List<String[]> records = csvFileProcess.getCSVfileData();
		
		//create the table STORE_ORDER
		DatabaseUtil.createTable();
		
		//CSV file data insert into database
		csvFileProcess.insertData(records);

	}
	
	public List<String[]> getCSVfileData() throws FileNotFoundException, IOException {
		List<String[]> records = new ArrayList<String[]>();
		try (CSVReader csvReader = new CSVReader(new FileReader("sales.csv"));) {
		    String[] values = null;
		    int count=1;
		    while ((values = csvReader.readNext()) != null) {
		       if(count!=1) // avoiding of header in CSV
		    	records.add(values);
		        count++;
		    }
		}
		return records;
	}
	
	
	public void insertData(List<String[]>storeList) throws Exception {
		Connection connection=null;
		PreparedStatement ps =null;
		try {
			connection= DatabaseUtil.getConnection();
			connection.setAutoCommit(false);  
			 ps = connection.prepareStatement(INSRT_SQL_QUERY);  
			
			for (String[] record : storeList) {
			    ps.setInt(1, Integer.valueOf(record[0]));
			    ps.setString(2, record[1]);
			    ps.setDate(3, getDateFormat(record[2].toString()));
			    ps.setDate(4, getDateFormat(record[3].toString()));
			    ps.setString(5,record[4]);
			    ps.setInt(6, Integer.valueOf(record[18]));
			    ps.setDouble(7, Double.parseDouble(record[19]));
			    ps.setDouble(8, round(Double.parseDouble(record[20].toString()), 2));
			    ps.setString(9, record[13]);
			    ps.setString(10, record[6]);
			    ps.setString(11, record[14]);
			    ps.setString(12, record[5]);
			    ps.setString(13, record[16]);
			    ps.addBatch();
			}
			ps.executeBatch();
			connection.commit();
			System.out.println("Data successfully saved in Database");
		} catch (Exception e) {
			System.out.println(e);
		}
		finally {
			ps.close();
		    connection.close();
		}
	}
   
	/*
	 * getdateFormat method need to use String date to java.util date
	 * 
	 */
	private Date getDateFormat(String dateStr) throws ParseException {
		return new java.sql.Date(
                ((java.util.Date)new SimpleDateFormat("dd.MM.yy").parse(dateStr)).getTime());
	}
	
	/*
	 * 
	 * round method is for round off the double values with 2 precision
	 */
	public static double round(double value, int places) {
	    if (places < 0) throw new IllegalArgumentException();

	    BigDecimal bd = BigDecimal.valueOf(value);
	    bd = bd.setScale(places, RoundingMode.HALF_UP);
	    return bd.doubleValue();
	}
	
	
	

}
