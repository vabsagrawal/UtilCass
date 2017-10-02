package com.vabs.validation;
import static com.nothing.utils.MarkdownConstants.cass_conn;

import com.datastax.driver.core.BoundStatement;

import static com.nothing.utils.MarkdownConstants.cass_conn_prod_sir;
import static com.nothing.utils.MarkdownConstants.cass_conn_qa_sir;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Batch;
import com.nothing.cassandra.CassandraConnection;
public class CassandraSelect_SIR {
	static Session session = null;
	static Session session_qa = null,session_prod=null;
	static BufferedReader items,stores,dest_type,dest_id,plans;
	static ArrayList<String> plist,blist,ilist,slist;
	static PreparedStatement query,insert_sir,select_sir;
	static String item,str2,str3;
	static ResultSet res_set_cass;
	static int store_size;
	static String destty,destid,item_no,store_no,plan_no;
	static String INSERT_USERSCENARIO="insert into storeprices.user_scenario_end_md_items (scenario_id,item_nbr,dest_type,dest_id) VALUES(a16e36e9-e3ca-454a-97d8-6c4b39bef6b6, ?,?, ?)";
	static String INSERT_SIR_SB = "INSERT INTO storeprices.store_item_retail (cntry_cd, div_nbr, item_nbr, store_nbr, effective_dt, expiration_dt, comment_cd, coop_fund, create_ts, creator_id, ho_rcmd_rtl, ho_rcmd_rtl_type, last_chg_ts, last_chg_user_id, legacy_update_flag, process_flag, store_rtl, store_rtl_type, unit_cost) VALUES('US', 1, ?, ?,'2017-04-20 05:30:00', '2049-12-31 05:30:00', '000000', 0, '2017-04-10 05:30:00', 'agajjar', 15, 'MD', '2017-04-10 05:30:00', 'agajjar','ILT','',NULL,'  ', 4.11)";
	static String Select_MD_ITEM_AGGR=null;
	private static BoundStatement bound_stmt = null;
	public static void main(String[] args) throws Exception{
		BufferedReader br = null;
		FileReader fr = null;
		br = new BufferedReader(new FileReader("C:/EclipseData/SIR_Items.txt"));
		cass_conn_qa_sir=CassandraConnection.getCassConn_SIR();
		session_qa=cass_conn_qa_sir.getSess_StorePrices_SIR();
//    	cass_conn_prod_sir=CassandraConnection.getCassConn_PROD_SIR();
//		session_prod=cass_conn_prod_sir.getSession2();
		String Select_SIR="SELECT count(*) FROM storeprices.store_item_retail where cntry_cd='US'and div_nbr=1 and item_nbr=?";
		select_sir=session_qa.prepare(Select_SIR);
		String item;
		 
		  try{
				
			  while ((item = br.readLine()) != null) {
					bound_stmt=select_sir.bind(Integer.parseInt(item));
					ResultSet resultSet = session_qa.execute(bound_stmt);
					List<Row> rows= resultSet.all();
					System.out.println(rows.get(0));
					
				}
			  }
			  catch(Exception e){
				  e.printStackTrace();
			  }
			  
			  finally{
					br.close();
					session_qa.close();
					
			  }
				

	}

}
