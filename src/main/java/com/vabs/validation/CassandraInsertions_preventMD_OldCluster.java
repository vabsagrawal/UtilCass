package com.vabs.validation;
import static com.nothing.utils.MarkdownConstants.cass_conn;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Random;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Batch;
import com.nothing.cassandra.CassandraConnection;
public class CassandraInsertions_preventMD_OldCluster {
	static Session session = null;
	static BufferedReader items,stores,dest_type,dest_id;
	static ArrayList<String> dest_type_list,dest_id_list,ilist,slist;
	static PreparedStatement query,insert_sir;
	static String item,str2,str3;
	static int store_size;
	static String destty,destid,item_no,store_no;
	static String INSERT_USERSCENARIO="INSERT INTO storeprices.pilot_items (item_nbr) VALUES(?)";
	//static String INSERT_USERSCENARIO="insert into storeprices.user_scenario_end_md_items (scenario_id,item_nbr,dest_type,dest_id) VALUES(a22d7c23-3160-4428-ac17-5c216b1f01f2, ?,'C','001eab45-3ab2-4f8f-ad8b-1af63c6b7e79')";
	//static String INSERT_USERSCENARIO="insert into storeprices.user_scenario_end_md_items (scenario_id,item_nbr,dest_type,dest_id) VALUES(a16e36e9-e3ca-454a-97d8-6c4b39bef6b6, ?,?, ?)";
	//static String INSERT_SIR_SB = "INSERT INTO storeprices.store_item_retail (cntry_cd, div_nbr, item_nbr, store_nbr, effective_dt, expiration_dt, comment_cd, coop_fund, create_ts, creator_id, ho_rcmd_rtl, ho_rcmd_rtl_type, last_chg_ts, last_chg_user_id, legacy_update_flag, process_flag, store_rtl, store_rtl_type, unit_cost) VALUES('US', 1, ?, ?,'2017-04-20 05:30:00', '2049-12-31 05:30:00', '000000', 0, '2017-04-10 05:30:00', 'agajjar', 15, 'MD', '2017-04-10 05:30:00', 'agajjar','ILT','',NULL,'  ', 4.11)";
	public static void main(String[] args) throws Exception{
		  cass_conn=CassandraConnection.getCassConn_pricechange();
		  BatchStatement batch = null;
		  session = cass_conn.getSess_markdown_perf();
		  try{
				items = new BufferedReader(new FileReader("C:/Items.txt"));
				stores = new BufferedReader(new FileReader("C:/Stores.txt")); 
				dest_type=new BufferedReader(new FileReader("C:/Dest_Type.txt"));
				dest_id=new BufferedReader(new FileReader("C:/Dest_ID.txt"));
				dest_type_list = new ArrayList<String>();
				dest_id_list = new ArrayList<String>();
				ilist = new ArrayList<String>();
				slist = new ArrayList<String>();
				Random rand = new Random(); 
				insert_sir = session.prepare(INSERT_USERSCENARIO);
				System.out.println("Starting Insert :" );
				long startTime = System.currentTimeMillis();
				item_no=items.readLine();
				System.out.println(item_no);
				batch = new BatchStatement();
				int i=0;
				while (item_no!=null)
				{	System.out.println("Inside while");
					long startBatchTime = System.currentTimeMillis();
						i++;
						batch.add(insert_sir.bind(Integer.parseInt(item_no)));
						if (i%50==0){
							session.executeAsync(batch);
							batch = new BatchStatement();
							System.out.println("Batch Execution Time for 50 insertions is :" +(System.currentTimeMillis() - startBatchTime));
							System.out.println(item_no);
						}
					
					item_no=items.readLine();
				}
				if (batch.size()>0)
					session.executeAsync(batch);
				System.out.println("Total Execution Time :" +(System.currentTimeMillis() - startTime));
		  }
		  
		  finally{
				items.close();
				stores.close();
				cass_conn.close();
		  }

	}

}
