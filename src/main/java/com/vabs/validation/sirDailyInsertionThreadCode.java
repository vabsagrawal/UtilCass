/**
 * 
 */
package com.vabs.validation;

import static com.nothing.utils.MarkdownConstants.cass_conn_prod_pricechange;
import static com.nothing.utils.MarkdownConstants.cass_conn_prod_sir;
import static com.nothing.utils.MarkdownConstants.cass_conn_qa_pricechange;
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
public class sirDailyInsertionThreadCode extends Thread {
	private List list1 = null;

	public sirDailyInsertionThreadCode(List list1) {
		this.list1 = list1;
	}

	public void run() {
		Session session_qa = null, session_prod = null;
		PreparedStatement insert_sir, select_sir, delete_sir;
		BoundStatement bound_stmt = null, bound_stmt_delete = null;
		try {
			System.out.println(Thread.currentThread().getName());
			cass_conn_qa_pricechange=CassandraConnection.getCassConn_dummy_pricechange();
			cass_conn_prod_pricechange=CassandraConnection.getCassConn_PROD_pricechange();
			session_qa=cass_conn_qa_pricechange.getSession5();
			session_prod=cass_conn_prod_pricechange.getSession3();
			
			System.out.println(session_qa);
			System.out.println(session_prod);
			
			ArrayList<storeItemRetailColumnDetails> sirDataList = new ArrayList<storeItemRetailColumnDetails>();
			
			String selectQuery = "SELECT * FROM storeprices.store_item_retail_daily where cntry_cd='US'and div_nbr=1 and item_nbr = ?";	
			select_sir=session_prod.prepare(selectQuery).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
			
			//String deleteQuery="DELETE FROM  storeprices.store_item_retail_daily WHERE cntry_cd='US' AND div_nbr=1 AND item_nbr=?";
			String deleteQuery="DELETE FROM  markdown.store_item_retail_daily_backup WHERE cntry_cd='US' AND div_nbr=1 AND item_nbr=?";
			delete_sir=session_qa.prepare(deleteQuery).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
			
			//String insertQuery = "INSERT INTO storeprices.store_item_retail_daily (cntry_cd, div_nbr, item_nbr, store_nbr, effective_dt, expiration_dt, comment_cd, coop_fund, create_ts, creator_id, ho_rcmd_rtl, ho_rcmd_rtl_type, last_chg_ts, last_chg_user_id, legacy_update_flag, notify_ind, process_flag, store_rtl, store_rtl_type, unit_cost) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?);";
			String insertQuery = "INSERT INTO markdown.store_item_retail_daily_backup (cntry_cd, div_nbr, item_nbr, store_nbr, effective_dt, expiration_dt, comment_cd, coop_fund, create_ts, creator_id, ho_rcmd_rtl, ho_rcmd_rtl_type, last_chg_ts, last_chg_user_id, legacy_update_flag, notify_ind, process_flag, store_rtl, store_rtl_type, unit_cost) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?);";
			insert_sir = session_qa.prepare(insertQuery).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
			
			System.out.println("Starting the Copy");
			String item;
			for (int j = 0; j < list1.size(); j++) {
				item = list1.get(j).toString();
				//System.out.println(item);
				bound_stmt = select_sir.bind(Integer.parseInt(item));
				ResultSet resultSet = session_prod.execute(bound_stmt);
				Iterator<Row> iterator = resultSet.iterator();
				while (iterator.hasNext()) {
					storeItemRetailColumnDetails sirData = new storeItemRetailColumnDetails();
					Row row = iterator.next();
					sirData.setCntry_cd(row.getString(0));
					sirData.setDiv_nbr(row.getInt(1));
					sirData.setItem_nbr(row.getInt(2));
					sirData.setStore_nbr(row.getInt(3));
					sirData.setEffective_dt(new Timestamp(row.getTimestamp(4)
							.getTime()));
					sirData.setExpiration_dt(new Timestamp(row.getTimestamp(5)
							.getTime()));
					sirData.setComment_cd(row.getString(6));
					sirData.setCoop_fund(row.getDecimal(7));
					sirData.setCreate_ts(new Timestamp(row.getTimestamp(8)
							.getTime()));
					sirData.setCreator_id(row.getString(9));
					sirData.setHo_rcmd_rtl(row.getDecimal(10));
					sirData.setHo_rcmd_rtl_type(row.getString(11));
					sirData.setLast_chg_ts(new Timestamp(row.getTimestamp(12)
							.getTime()));
					sirData.setLast_chg_user_id(row.getString(13));
					sirData.setLegacy_update_flag(row.getString(14));
					sirData.setNotify_ind(row.getInt(15));
					sirData.setProcess_flag(row.getString(16));
					sirData.setStore_rtl(row.getDecimal(17));
					sirData.setStore_rtl_type(row.getString(18));
					sirData.setUnit_cost(row.getDecimal(19));
					sirDataList.add(sirData);
				}
				System.out.println("Item  Count in PROD SIR_DAILY is " + item + "- "
						+ sirDataList.size());
				bound_stmt_delete = delete_sir.bind(Integer.parseInt(item));
				session_qa.execute(bound_stmt_delete);
				System.out.println("Item -" + item + " is deleted from QA SIR_DAILY");
				int i = 1;
				BatchStatement batch = new BatchStatement();
				int numFutures = 0;
				List<ResultSetFuture> futures = new ArrayList<ResultSetFuture>();
				for (storeItemRetailColumnDetails sir_row_list : sirDataList) {
					batch.add(insert_sir.bind(sir_row_list.getCntry_cd(),
							sir_row_list.getDiv_nbr(),
							sir_row_list.getItem_nbr(),
							sir_row_list.getStore_nbr(),
							sir_row_list.getEffective_dt(),
							sir_row_list.getExpiration_dt(),
							sir_row_list.getComment_cd(),
							sir_row_list.getCoop_fund(),
							sir_row_list.getCreate_ts(),
							sir_row_list.getCreator_id(),
							sir_row_list.getHo_rcmd_rtl(),
							sir_row_list.getHo_rcmd_rtl_type(),
							sir_row_list.getLast_chg_ts(),
							sir_row_list.getLast_chg_user_id(),
							sir_row_list.getLegacy_update_flag(),
							sir_row_list.getNotify_ind(),
							sir_row_list.getProcess_flag(),
							sir_row_list.getStore_rtl(),
							sir_row_list.getStore_rtl_type(),
							sir_row_list.getUnit_cost()));
					i++;
					if (i % 50 == 0) {
						ResultSetFuture future = session_qa.executeAsync(batch);
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
					session_qa.executeAsync(batch);
				}
				System.out.println("Item Insertion in QA SIR_DAILY is completed for item "
						+ item);
				sirDataList = new ArrayList<storeItemRetailColumnDetails>();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		finally{
			session_qa.close();
			session_prod.close();		
			cass_conn_qa_sir.close();
			cass_conn_prod_sir.close();
			System.out.println("Inside Finally Block");
		}
	}
}