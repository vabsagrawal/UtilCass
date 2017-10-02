package com.vabs.validation;

import static com.nothing.utils.MarkdownConstants.prop;
import static com.nothing.utils.MarkdownConstants.hana_conn;
import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import com.nothing.Properties.PropertyLoader;

public class HanaConnection {

	static
	  {
		try{
		Class.forName("com.sap.db.jdbc.Driver");
	       }
		catch(Exception e){
			e.printStackTrace();
		}
	  }
	public static Connection hanaConnection() throws Exception{
		prop=PropertyLoader.propLoad();
		hana_conn=DriverManager.getConnection(prop.getProperty("hana_dburl"),prop.getProperty("hana_userid"),prop.getProperty("hana_password"));
		System.out.println("SAP Hana Connection Established");
		return hana_conn;
	}
}
