package com.vabs.validation;
import static com.nothing.utils.MarkdownConstants.prop;

import org.apache.commons.lang3.StringUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.nothing.Properties.PropertyLoader;

public class CassandraConnection {
	//private static final Logger LOGGER = Logger.getLogger(CassandraConnection.class.getName());
	   private Cluster cluster;
	   private static Session sess_markdown_perf,sess_storepriceskeyspace_SIR, session2,session3,sess_StorePrice_cert,session5;
	   
	   //***************For Cassandra markdown Keyspace*****************//
	   public void markdownkeyspace_connect(final String node, final int port, final String keyspace)
	   {
		   String serverIPs[] = node.split(",");
			for (int ipIdx = 0; ipIdx < serverIPs.length; ipIdx++) {
				serverIPs[ipIdx] = StringUtils.trim(serverIPs[ipIdx]);
			}
	      this.cluster = Cluster.builder().addContactPoints(serverIPs).withPort(9042)
	    		  .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM))
	    		  .withSocketOptions(new SocketOptions().setReadTimeoutMillis(100000))
	    		  .withCredentials(prop.getProperty("cass_markdownkeyspace_userID"), prop.getProperty("cass_markdownkeyspace_password")).withProtocolVersion(ProtocolVersion.V3).build();
	      //this.cluster = Cluster.builder().addContactPoints(serverIPs).withPort(9042).withCredentials("NGPMDUSWM", "pTE8MNWh9DqrDcMW").withProtocolVersion(ProtocolVersion.V3).build();
			final Metadata metadata = cluster.getMetadata();
	 	  sess_markdown_perf = cluster.connect(keyspace);
	   }
	   
	 //***************For Cassandra storeprices Keyspace-SIR*****************//
	   public void storepriceskeyspace_SIR_connect(final String node, final int port, final String keyspace)
	   {
		   String serverIPs[] = node.split(",");
			for (int ipIdx = 0; ipIdx < serverIPs.length; ipIdx++) {
				serverIPs[ipIdx] = StringUtils.trim(serverIPs[ipIdx]);
			}
	      this.cluster = Cluster.builder().addContactPoints(serverIPs).withPort(9042).withCredentials(prop.getProperty("cass_storepriceskeyspace_userID"), prop.getProperty("cass_storepriceskeyspace_password")).withProtocolVersion(ProtocolVersion.V3).build();
			final Metadata metadata = cluster.getMetadata();
	 	  sess_storepriceskeyspace_SIR = cluster.connect(keyspace);
	   }
	   
	 //***************For Cassandra storeprices Keyspace-Price Change*****************//
	   public void storepriceskeyspace_pricechange_connect(final String node, final int port, final String keyspace)
	   {
		   String serverIPs[] = node.split(",");
			for (int ipIdx = 0; ipIdx < serverIPs.length; ipIdx++) {
				serverIPs[ipIdx] = StringUtils.trim(serverIPs[ipIdx]);
			}
	      this.cluster = Cluster.builder().addContactPoints(serverIPs).withPort(9042).withCredentials(prop.getProperty("cass_storepriceskeyspace_pc_userID"), prop.getProperty("cass_storepriceskeyspace_pc_password")).withProtocolVersion(ProtocolVersion.V3).build();
	      //this.cluster = Cluster.builder().addContactPoints(serverIPs).withPort(9042).withCredentials("NGPMDUSWM", "pTE8MNWh9DqrDcMW").withProtocolVersion(ProtocolVersion.V3).build();
			final Metadata metadata = cluster.getMetadata();
	 	  sess_StorePrice_cert = cluster.connect(keyspace);
	   }
	   
		 //***************For Cassandra PROD storeprices Keyspace-SIR*****************//
	   public void storepriceskeyspace_prod_SIR_connect(final String node, final int port, final String keyspace)
	   {
		   String serverIPs[] = node.split(",");
			for (int ipIdx = 0; ipIdx < serverIPs.length; ipIdx++) {
				serverIPs[ipIdx] = StringUtils.trim(serverIPs[ipIdx]);
			}
	      this.cluster = Cluster.builder().addContactPoints(serverIPs).withPort(9042).withCredentials(prop.getProperty("cass_storepriceskeyspace_prod_userID"), prop.getProperty("cass_storepriceskeyspace_prod_password")).withProtocolVersion(ProtocolVersion.V3).build();
	      //this.cluster = Cluster.builder().addContactPoints(serverIPs).withPort(9042).withCredentials("NGPMDUSWM", "pTE8MNWh9DqrDcMW").withProtocolVersion(ProtocolVersion.V3).build();
			final Metadata metadata = cluster.getMetadata();
	 	  session2 = cluster.connect(keyspace);
	   }
	   
		 //***************For Cassandra PROD storeprices Keyspace pricechange-SIR*****************//
	   public void storepriceskeyspace_prod_pricechange_connect(final String node, final int port, final String keyspace)
	   {
		   String serverIPs[] = node.split(",");
			for (int ipIdx = 0; ipIdx < serverIPs.length; ipIdx++) {
				serverIPs[ipIdx] = StringUtils.trim(serverIPs[ipIdx]);
			}
	      this.cluster = Cluster.builder().addContactPoints(serverIPs).withPort(9042).withCredentials(prop.getProperty("cass_storepriceskeyspace_pc_prod_userID"), prop.getProperty("cass_storepriceskeyspace_pc_prod_password")).withProtocolVersion(ProtocolVersion.V3).build();
	      //this.cluster = Cluster.builder().addContactPoints(serverIPs).withPort(9042).withCredentials("NGPMDUSWM", "pTE8MNWh9DqrDcMW").withProtocolVersion(ProtocolVersion.V3).build();
			final Metadata metadata = cluster.getMetadata();
	 	  session3 = cluster.connect(keyspace);
	   }
	   
		 //***************For Cassandra storeprices Keyspace pricechange-SIR*****************//
	   public void storepriceskeyspace_dummy_pricechange_connect(final String node, final int port, final String keyspace)
	   {
		   String serverIPs[] = node.split(",");
			for (int ipIdx = 0; ipIdx < serverIPs.length; ipIdx++) {
				serverIPs[ipIdx] = StringUtils.trim(serverIPs[ipIdx]);
			}
	      this.cluster = Cluster.builder().addContactPoints(serverIPs).withPort(9042).withCredentials(prop.getProperty("cass_storepriceskeyspace_pc_dummy_userID"), prop.getProperty("cass_storepriceskeyspace_pc_dummy_password")).withProtocolVersion(ProtocolVersion.V3).build();
	      //this.cluster = Cluster.builder().addContactPoints(serverIPs).withPort(9042).withCredentials("NGPMDUSWM", "pTE8MNWh9DqrDcMW").withProtocolVersion(ProtocolVersion.V3).build();
			final Metadata metadata = cluster.getMetadata();
	 	  session5 = cluster.connect(keyspace);
	   }
	   
	   public Session getSess_StorePrices_SIR()
	   {
	      return this.sess_storepriceskeyspace_SIR;
	   }
	   public Session getSession2()
	   {
	      return this.session2;
	   }
	   public Session getSession3()
	   {
	      return this.session3;
	   }
	   public Session getSess_StorePrice_cert()
	   {
	      return this.sess_StorePrice_cert;
	   }
	   public Session getSession5()
	   {
	      return this.session5;
	   }
	   public Session getSess_markdown_perf()
	   {
	      return this.sess_markdown_perf;
	   }
	   public void close()
	   {
	      cluster.close();
	   }
	   
	 //***************For Cassandra markdown Keyspace*****************//
	   public static CassandraConnection getCassConn_markdown() throws Exception
		{
		   prop=PropertyLoader.propLoad();
		   CassandraConnection con = new CassandraConnection();
		   con.markdownkeyspace_connect(prop.getProperty("cass_markdownkeyspace_url"), 9042,prop.getProperty("cass_markdownkeyspace_keyspace"));
		   
		   //con.connect("10.246.73.222,10.246.74.159,10.246.74.31,10.246.74.156,10.246.73.220,10.246.74.29,10.246.74.26,10.246.74.24,10.246.74.153,10.246.73.217", 9042,"markdown");
		   System.out.println("Cassandra Connection Established for markdown keyspace");
		   return con;
		}
	   
	 //***************For Cassandra storeprices Keyspace-SIR*****************//
	   public static CassandraConnection getCassConn_SIR() throws Exception
		{
		   prop=PropertyLoader.propLoad();
		   CassandraConnection con = new CassandraConnection();
		   con.storepriceskeyspace_SIR_connect(prop.getProperty("cass_storepriceskeyspace_url"), 9042,prop.getProperty("cass_storepriceskeyspace_keyspace"));
		   System.out.println("Cassandra Connection Established for storeprices keyspace - SIR");
		  // PropertyLoader.close();
		   return con;
		}
	   
	 //***************For Cassandra storeprices Keyspace-Price Change*****************//
	   public static CassandraConnection getCassConn_pricechange() throws Exception
		{
		   prop=PropertyLoader.propLoad();
		   CassandraConnection con = new CassandraConnection();
		   con.storepriceskeyspace_pricechange_connect(prop.getProperty("cass_storepriceskeyspace_pc_url"), 9042,prop.getProperty("cass_storepriceskeyspace_pc_keyspace"));
		   System.out.println("Cassandra Connection Established for storeprices keyspace - Price Change");
		   //PropertyLoader.close();
		   return con;
		}
	   
	 //***************For PROD Cassandra storeprices Keyspace-SIR*****************//
	   public static CassandraConnection getCassConn_PROD_SIR() throws Exception
		{
		   prop=PropertyLoader.propLoad();
		   CassandraConnection con = new CassandraConnection();
		   con.storepriceskeyspace_prod_SIR_connect(prop.getProperty("cass_storepriceskeyspace_prod_url"), 9042,prop.getProperty("cass_storepriceskeyspace_prod_keyspace"));
		   System.out.println("Cassandra Connection Established for PROD storeprices keyspace - SIR");
		   return con;
		}
	   
		 //***************For PROD Cassandra storeprices Keyspace-Price Change*****************//
	   public static CassandraConnection getCassConn_PROD_pricechange() throws Exception
		{
		   prop=PropertyLoader.propLoad();
		   CassandraConnection con = new CassandraConnection();
		   con.storepriceskeyspace_prod_pricechange_connect(prop.getProperty("cass_storepriceskeyspace_pc_prod_url"), 9042,prop.getProperty("cass_storepriceskeyspace_pc_prod_keyspace"));
		   System.out.println("Cassandra Connection Established for PROD storeprices keyspace - Price Change");
		   return con;
		}
	   
		 //***************For Dummy Cassandra markdown Keyspace-Price Change*****************//
	   public static CassandraConnection getCassConn_dummy_pricechange() throws Exception
		{
		   prop=PropertyLoader.propLoad();
		   CassandraConnection con = new CassandraConnection();
		   con.storepriceskeyspace_dummy_pricechange_connect(prop.getProperty("cass_storepriceskeyspace_pc_dummy_url"), 9042,prop.getProperty("cass_storepriceskeyspace_pc_dummy_keyspace"));
		   System.out.println("Cassandra Connection Established for Dummy markdown keyspace - Price Change");
		   return con;
		}
}
