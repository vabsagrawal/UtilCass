package com.vabs.validation;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.testng.annotations.*;

import java.util.*;
import java.util.logging.*;

import com.nothing.cassandra.CassandraConnection;
import com.nothing.mysql.MysqlConnection;
import com.nothing.sap.HanaConnection;
import com.nothing.teradata.TeradataConnection;

import static com.nothing.utils.MarkdownConstants.mysql_conn;
import static com.nothing.utils.MarkdownConstants.cass_conn;
import static com.nothing.utils.MarkdownConstants.terdata_conn;
import static com.nothing.utils.MarkdownConstants.hana_conn;
public class TeradataBatch{
  private static final Logger LOGGER = Logger.getLogger(TeradataBatch.class.getName());
  @Test
  public void test1() {
	  LOGGER.info("Inside Test Method");
  }
  @BeforeMethod
  public void beforeMethod() {
  }

  @AfterMethod
  public void afterMethod() {
  }

  @BeforeClass
  public void beforeClass() throws Exception{
	  mysql_conn=MysqlConnection.mysqlConnection();
	  cass_conn=CassandraConnection.getCassConn_markdown();
	  cass_conn=CassandraConnection.getCassConn_SIR();
	  cass_conn=CassandraConnection.getCassConn_pricechange();
	  terdata_conn=TeradataConnection.tdConnection();
	  hana_conn=HanaConnection.hanaConnection();
  }

  @AfterClass
  public void afterClass() {
  }

  @BeforeTest
  public void beforeTest() {
  }

  @AfterTest
  public void afterTest() {
  }

  @BeforeSuite
  public void beforeSuite() {
  }

  @AfterSuite
  public void afterSuite() {
  }

}
