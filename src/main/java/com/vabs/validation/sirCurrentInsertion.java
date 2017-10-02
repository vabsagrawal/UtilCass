/**
 * 
 */
package com.vabs.validation;
import static com.nothing.utils.MarkdownConstants.cass_conn_qa_sir;
import static com.nothing.utils.MarkdownConstants.cass_conn_qa_pricechange;
import static com.nothing.utils.MarkdownConstants.cass_conn_prod_pricechange;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static com.nothing.utils.MarkdownConstants.cass_conn_prod_sir;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.mysql.jdbc.Statement;
import com.nothing.cassandra.CassandraConnection;
/**
 * @author v0b003r
 *
 */
public class sirCurrentInsertion {
	private static final String FILENAME = System.getProperty("user.dir")+"/D721ItemsOnMD.txt";
	static ResultSet res_set_cass;
	static Session session_qa = null,session_prod=null;
	static PreparedStatement query,insert_sir,select_sir,delete_sir;
	private static Integer batchSize = 50;
	private static Integer writeBatchCommitCount = 20;
	private static BoundStatement bound_stmt = null,bound_stmt_delete=null;
	static BufferedReader qa_sir_items;
	static String delete_item=null;
	static List noDataItems = new ArrayList<>();
	static int sItems =0;
	public static void main(String[] args) throws Exception{
		BufferedReader br = null;
		FileReader fr = null;
		br = new BufferedReader(new FileReader(FILENAME));
		BatchStatement batch = null;
		cass_conn_qa_sir=CassandraConnection.getCassConn_SIR();
		cass_conn_prod_sir=CassandraConnection.getCassConn_PROD_SIR();
		session_qa=cass_conn_qa_sir.getSess_StorePrices_SIR();
		session_prod=cass_conn_prod_sir.getSession2();
		System.out.println(session_qa);
		System.out.println(session_prod);
		System.out.println(session_qa);
		System.out.println(session_prod);
		ArrayList<storeItemRetailCurrentColumnDetails> sirDataList = new ArrayList<storeItemRetailCurrentColumnDetails>();
		String selectQuery = "SELECT * FROM storeprices.item_store_retail_curr where item_nbr = ?";	
		select_sir=session_prod.prepare(selectQuery).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
		String deleteQuery="DELETE FROM  storeprices.item_store_retail_curr where item_nbr = ?";
		delete_sir=session_qa.prepare(deleteQuery).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
		String insertQuery = "INSERT INTO storeprices.item_store_retail_curr (item_nbr, store_nbr, effective_dt, expiration_dt, ho_rcmd_rtl, ho_rcmd_rtl_type) VALUES(?, ?, ?, ?, ?, ?);";
		insert_sir = session_qa.prepare(insertQuery).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
		System.out.println("Starting the Copy");
		String item;
		try{
		while ((item = br.readLine()) != null) {
			bound_stmt=select_sir.bind(Integer.parseInt(item));
			ResultSet resultSet = session_prod.execute(bound_stmt);
			
		Iterator<Row> iterator = resultSet.iterator();
		if(!iterator.hasNext()){
			noDataItems.add(item);
			continue;
		}
		while(iterator.hasNext())
		{
			storeItemRetailCurrentColumnDetails sirData = new storeItemRetailCurrentColumnDetails();
			Row row = iterator.next();
			sirData.setItem_nbr(row.getInt(0));
			sirData.setStore_nbr(row.getInt(1));
			sirData.setEffective_dt(new Timestamp(row.getTimestamp(2).getTime()));
			sirData.setExpiration_dt(new Timestamp(row.getTimestamp(3).getTime()));
			sirData.setHo_rcmd_rtl(row.getDecimal(4));
			sirData.setHo_rcmd_rtl_type(row.getString(5));
			sirDataList.add(sirData);
		}	
		System.out.println("Item  Count in PROD SIR Current is "+item+"- "+sirDataList.size());
		bound_stmt_delete=delete_sir.bind(Integer.parseInt(item));
		session_qa.execute(bound_stmt_delete);
		System.out.println("Item -"+item+" is deleted from QA Current");
		int i=1;
		batch = new BatchStatement();
		int numFutures =0;
		List<ResultSetFuture> futures = new ArrayList<ResultSetFuture>();
		for(storeItemRetailCurrentColumnDetails sir_row_list : sirDataList){
			batch.add(insert_sir.bind(sir_row_list.getItem_nbr(),sir_row_list.getStore_nbr(),sir_row_list.getEffective_dt(),sir_row_list.getExpiration_dt(),sir_row_list.getHo_rcmd_rtl(),sir_row_list.getHo_rcmd_rtl_type()));
			i++;
			if (i%50==0){
				ResultSetFuture future = session_qa.executeAsync(batch);
				numFutures++;
				futures.add(future);
				if(numFutures==5){
					//System.out.println("Concurrent Batch Count Reaches 20");
					for(ResultSetFuture f : futures){
						f.get();
					}
					numFutures=0;
					futures = new ArrayList<ResultSetFuture>();
				}
				batch = new BatchStatement();
			}
			
		}
		if (batch.size()>0){
			session_qa.executeAsync(batch);
		}
		System.out.println("Item Insertion in QA SIR Current is completed for item "+item);
		sItems++;
		sirDataList=new ArrayList<storeItemRetailCurrentColumnDetails>();
	      }
		System.out.println(noDataItems + ":::::size::::" + noDataItems.size());
		System.out.println("SIR Current Data Migration Completed For items with Size:::::: "+ sItems);
		}
		catch(Exception e){
			e.printStackTrace();
			
		}
		
		finally{
			System.out.println("Inside Finally Block");
			session_qa.close();
			session_prod.close();
			br.close();
			
		}
	}
}
