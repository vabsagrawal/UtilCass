package com.vabs.validation;

import java.io.*;
import java.sql.*;
import java.util.*;

import com.datastax.driver.core.*;
import com.nothing.cassandra.CassandraConnection;
public class MarkdownConstants {
	
	public static Connection mysql_conn=null,terdata_conn=null,hana_conn=null;
	//public static Connection hana_conn=null;
	public static Properties prop=new Properties();
	public static FileInputStream fis=null;
	public static CassandraConnection cass_conn= new CassandraConnection();
	public static CassandraConnection cass_conn_qa_sir= new CassandraConnection();
	public static CassandraConnection cass_conn_prod_sir= new CassandraConnection();
	public static CassandraConnection cass_conn_qa_pricechange= new CassandraConnection();
	public static CassandraConnection cass_conn_prod_pricechange= new CassandraConnection();
}
