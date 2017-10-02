/**
 * 
 */
package com.vabs.validation;

import static com.nothing.utils.MarkdownConstants.cass_conn_prod_sir;
import static com.nothing.utils.MarkdownConstants.cass_conn_qa_sir;

import java.io.BufferedReader;
import java.sql.Timestamp;
import java.util.ArrayList;
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
import com.nothing.cassandra.CassandraConnection;

/**
 * @author v0b003r
 * 
 */
public class sirCurrentInsertionThreadCode extends Thread {
	private List list1 = null;

	public sirCurrentInsertionThreadCode(List list1) {
		this.list1 = list1;
	}

	public void run() {
		Session session_qa = null, session_prod = null;
		PreparedStatement insert_sir, select_sir, delete_sir;
		BoundStatement bound_stmt = null, bound_stmt_delete = null;
		try {
			System.out.println(Thread.currentThread().getName());
			cass_conn_qa_sir = CassandraConnection.getCassConn_SIR();
			cass_conn_prod_sir = CassandraConnection.getCassConn_PROD_SIR();
			session_qa = cass_conn_qa_sir.getSess_StorePrices_SIR();
			session_prod = cass_conn_prod_sir.getSession2();
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
			System.out.println(list1);
			for (int j = 0; j < list1.size(); j++) {
				item = list1.get(j).toString();
				System.out.println(item);
				bound_stmt = select_sir.bind(Integer.parseInt(item));
				ResultSet resultSet = session_prod.execute(bound_stmt);
				Iterator<Row> iterator = resultSet.iterator();
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
				System.out.println("Item  Count in PROD SIR_Current is " + item + "- "
						+ sirDataList.size());
				bound_stmt_delete = delete_sir.bind(Integer.parseInt(item));
				session_qa.execute(bound_stmt_delete);
				System.out.println("Item -" + item + " is deleted from QA SIR_Current");
				int i = 1;
				BatchStatement batch = new BatchStatement();
				int numFutures = 0;
				List<ResultSetFuture> futures = new ArrayList<ResultSetFuture>();
				ResultSetFuture future = null;
				for (storeItemRetailCurrentColumnDetails sir_row_list : sirDataList) {
					batch.add(insert_sir.bind(sir_row_list.getItem_nbr(),sir_row_list.getStore_nbr(),sir_row_list.getEffective_dt(),sir_row_list.getExpiration_dt(),sir_row_list.getHo_rcmd_rtl(),sir_row_list.getHo_rcmd_rtl_type()));
					i++;
					if (i % 50 == 0) {
						 future = session_qa.executeAsync(batch);
						numFutures++;
						futures.add(future);
						if (numFutures == 20) {
							// System.out.println("Concurrent Batch Count Reaches 20");
							for (ResultSetFuture f : futures) {
								f.get();
							}
							numFutures = 0;
							futures = new ArrayList<ResultSetFuture>();
						}
						batch = new BatchStatement();
					}

				}
				if (batch.size() > 0) {
					future = session_qa.executeAsync(batch);
					 for (ResultSetFuture f : futures) {
							f.get();
						}
				}
				System.out.println("Item Insertion in QA SIR_Current is completed for item "
						+ item);
				sirDataList = new ArrayList<storeItemRetailCurrentColumnDetails>();
			 }
			} catch (Exception e) {
				e.printStackTrace();
		}
		finally{
//			session_qa.close();
//			session_prod.close();		
//			cass_conn_qa_sir.close();
//			cass_conn_prod_sir.close();
			System.out.println("Inside Finally Block");
		}
	}
}