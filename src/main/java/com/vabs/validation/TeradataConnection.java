package com.vabs.validation;

import static com.nothing.utils.MarkdownConstants.prop;
import static com.nothing.utils.MarkdownConstants.terdata_conn;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import com.nothing.Properties.PropertyLoader;

public class TeradataConnection {
	static
	  {
		try{
				Class.forName("com.teradata.jdbc.TeraDriver");
	       }
		catch(Exception e){
			e.printStackTrace();
		}
	 }
	public static Connection tdConnection() throws Exception{
		prop=PropertyLoader.propLoad();
		terdata_conn=DriverManager.getConnection(prop.getProperty("teradata_dburl"),prop.getProperty("teradata_userid"),prop.getProperty("teradata_password"));
		System.out.println("Teradata Connection Established");
		return terdata_conn;
	}
	public static void close(Connection conn,Statement stmt,ResultSet rs){
		System.out.println("Inside close method");
		try{
			if(conn!=null){
				conn.close(); }
			if(stmt!=null){
				stmt.close(); }
			if(rs!=null){
			rs.close(); }
			System.out.println("Closed all the teradata resources");
			}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}
