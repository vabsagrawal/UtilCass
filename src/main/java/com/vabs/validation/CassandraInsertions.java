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
public class CassandraInsertions {
	static Session session = null;
	static BufferedReader items,stores,dest_type,dest_id;
	static ArrayList<String> alist,blist,ilist,slist;
	static PreparedStatement query,insert_sir;
	static String item,str2,str3;
	static int store_size;
	static String destty,destid,item_no,store_no;
	static String INSERT_USERSCENARIO="insert into storeprices.user_scenario_end_md_items (scenario_id,item_nbr,dest_type,dest_id) VALUES(a16e36e9-e3ca-454a-97d8-6c4b39bef6b6, ?,?, ?)";
	static String INSERT_SIR_SB = "INSERT INTO storeprices.store_item_retail (cntry_cd, div_nbr, item_nbr, store_nbr, effective_dt, expiration_dt, comment_cd, coop_fund, create_ts, creator_id, ho_rcmd_rtl, ho_rcmd_rtl_type, last_chg_ts, last_chg_user_id, legacy_update_flag, process_flag, store_rtl, store_rtl_type, unit_cost) VALUES('US', 1, ?, ?,'2017-04-20 05:30:00', '2049-12-31 05:30:00', '000000', 0, '2017-04-10 05:30:00', 'agajjar', 15, 'MD', '2017-04-10 05:30:00', 'agajjar','ILT','',NULL,'  ', 4.11)";
	public static void main(String[] args) throws Exception{
		  cass_conn=CassandraConnection.getCassConn_SIR();
		  BatchStatement batch = null;
		  session = cass_conn.getSess_markdown_perf();
		  try{
				items = new BufferedReader(new FileReader("C:/Items.txt"));
				stores = new BufferedReader(new FileReader("C:/Stores.txt")); 
//				dest_type=new BufferedReader(new FileReader("C:/Dest_Type.txt"));
//				dest_id=new BufferedReader(new FileReader("C:/Dest_ID.txt"));
				alist = new ArrayList<String>();
				blist = new ArrayList<String>();
				ilist = new ArrayList<String>();
				slist = new ArrayList<String>();
				Random rand = new Random(); 
				insert_sir = session.prepare(INSERT_SIR_SB);
				while ((store_no=stores.readLine())!=null)
				{
					System.out.println(store_no);
					slist.add(store_no);
				}
				store_size=slist.size();
				System.out.println("Size of Stores-ArrayList is "+store_size);
				System.out.println("Stores-ArrayList is "+slist);
				System.out.println("Starting Insert :" );
				long startTime = System.currentTimeMillis();
				item_no=items.readLine();
				batch = new BatchStatement();
				while (item_no!=null)
				{	
					for (int i=0;i<store_size;i++)
					{
						long startBatchTime = System.currentTimeMillis();
						store_no=slist.get(i);
						System.out.println(item_no+" "+store_no);
						//batch.add(insert_sir.bind(new Integer(item_no),new Integer(store_no)));
						batch.add(insert_sir.bind(Integer.parseInt(item_no),Integer.parseInt(store_no)));
						if (i%50==0){
							session.executeAsync(batch);
							batch = new BatchStatement();
							System.out.println("Batch Execution Time for 50 insertions is :" +(System.currentTimeMillis() - startBatchTime));
						}
					}
					if (batch.size()>0)
						session.executeAsync(batch);
					item_no=items.readLine();
				}
				System.out.println("Total Execution Time :" +(System.currentTimeMillis() - startTime));
		  }
		  
		  finally{
				items.close();
				stores.close();
				cass_conn.close();
		  }

	}

}
