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
public class CassandraInsertions_LiftingMD {
	static Session session = null;
	static BufferedReader items,stores,dest_type,dest_id;
	static ArrayList<String> alist,blist,ilist,slist;
	static PreparedStatement query,insert_sir;
	static String item,str2,str3;
	static int store_size;
	static String destty,destid,item_no,store_no;
	//static String INSERT_USERSCENARIO="insert into storeprices.user_scenario_end_md_items (scenario_id,item_nbr,dest_type,dest_id) VALUES(a16e36e9-e3ca-454a-97d8-6c4b39bef6b6, ?,?, ?)";
	static String INSERT_itemstorecleanup="INSERT INTO markdown.item_store_cleaned (effective_date, item_nbr, store_nbr, future_item_store_ind, gs_ind, on_another_module_ind, po_ind, recall_ind, relo_ind, replenishable_ind, store_item_ind) VALUES(20170630, ?, ?, false, false, false, false, false, true, false, true);";
	public static void main(String[] args) throws Exception{
		  cass_conn=CassandraConnection.getCassConn_markdown();
		  BatchStatement batch = null;
		  session = cass_conn.getSess_markdown_perf();
		  try{
				items = new BufferedReader(new FileReader("C:/EclipseData/LiftingMD_Items.txt"));
				stores = new BufferedReader(new FileReader("C:/EclipseData/Stores_5K_linebyline.txt")); 
				ilist = new ArrayList<String>();
				slist = new ArrayList<String>();
				insert_sir = session.prepare(INSERT_itemstorecleanup);
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
