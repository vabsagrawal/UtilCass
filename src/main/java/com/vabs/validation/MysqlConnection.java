package com.vabs.validation;

import java.io.*;
import java.sql.*;
import java.util.*;

import org.apache.log4j.Logger;

import com.nothing.Properties.PropertyLoader;

import static com.nothing.utils.MarkdownConstants.mysql_conn;
import static com.nothing.utils.MarkdownConstants.prop;

public class MysqlConnection {
	//private static final Logger LOGGER = Logger.getLogger(MysqlConnection.class.getName());
	static
	  {
		try{
		Class.forName("com.mysql.jdbc.Driver");
	       }
		catch(Exception e){
			e.printStackTrace();
		}
	  }
	
	public static Connection mysqlConnection() throws Exception{
		prop=PropertyLoader.propLoad();
		mysql_conn=DriverManager.getConnection(prop.getProperty("mysql_jdbc_url"), prop.getProperty("mysql_userid"), prop.getProperty("mysql_password"));  
		//LOGGER.info("MYSQL Connection Established");
		System.out.println("MYSQL Connection Established");
		return mysql_conn;
	}
	public static void close(Connection conn,Statement stmt,ResultSet rs){
	try
	  {
		if(conn!=null){
			conn.close();
		}
		if(stmt!=null){
			stmt.close();
		}
		if(rs!=null){
			rs.close();
		}
	  }
	catch(Exception e)
	  {
		System.out.println(e);	
	  }
	}	
}
