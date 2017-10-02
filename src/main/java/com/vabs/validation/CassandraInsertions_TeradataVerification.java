package com.vabs.validation;
import static com.nothing.utils.MarkdownConstants.cass_conn;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Random;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Batch;
import com.nothing.cassandra.CassandraConnection;
import com.datastax.driver.core.ResultSet;
public class CassandraInsertions_TeradataVerification {
	static Session session = null;
	static BufferedReader items,stores,dest_type,dest_id;
	static ArrayList<String> alist,blist,ilist,slist;
	static PreparedStatement query,insert_sir;
	static String item,str2,str3;
	static ResultSet res_set_cass;
	static int store_size,res_set_cass_size,res_set_curr_size,i=1;
	static String destty,destid,item_no,store_no;
	static String INSERT_USERSCENARIO="insert into storeprices.user_scenario_end_md_items (scenario_id,item_nbr,dest_type,dest_id) VALUES(a16e36e9-e3ca-454a-97d8-6c4b39bef6b6, ?,?, ?)";
	static String select_itemstorecleanup;
	static StringBuilder sb_itemstorecleanup;
	public static void main(String[] args) throws Exception{
		  cass_conn=CassandraConnection.getCassConn_markdown();
		  session = cass_conn.getSess_markdown_perf();
			long startTime = System.currentTimeMillis();
	/*	  try{
				items = new BufferedReader(new FileReader("C:/EclipseData/TD_Items.txt"));
				item_no=items.readLine();
				res_set_curr_size=0;
				while (item_no!=null)
				{	
					select_itemstorecleanup="select  *  from markdown.item_store_cleaned where item_nbr="+item_no+" and store_nbr<9000 ALLOW FILTERING";
					res_set_cass=session.execute(select_itemstorecleanup);
					res_set_cass_size=res_set_cass.all().size();
					System.out.println("No of cleanup stores for item-"+(i++)+" "+item_no+" is "+res_set_cass_size);
					res_set_curr_size+=res_set_cass_size;
					item_no=items.readLine();
					Thread.sleep(1000);
				}

		  }
		  catch(Exception e){
			  e.printStackTrace();
		  }
		  
		  finally{
				System.out.println("Total no of rows present in item_store_cleanup table is "+res_set_curr_size);
				System.out.println("Total Execution Time :" +(System.currentTimeMillis() - startTime));
				items.close();
				cass_conn.close();
		  }*/
		  
		  
		  /*
		  //Below StringBuilder code taking more time compared to above code
		  sb_itemstorecleanup=new StringBuilder("select * from markdown.item_store_cleaned where item_nbr=");
		  try{
				items = new BufferedReader(new FileReader("C:/EclipseData/TD_Items.txt"));
				item_no=items.readLine();
				long startTime = System.currentTimeMillis();
				while (item_no!=null)
				{	
					sb_itemstorecleanup.append(item_no);
					sb_itemstorecleanup.append(" and store_nbr<9000 ALLOW FILTERING;");
					//System.out.println(sb_itemstorecleanup);
					res_set_cass=session.execute(sb_itemstorecleanup.toString());
					res_set_cass_size=res_set_cass.all().size();
					System.out.println("ResultSet Size of Item-"+item_no+" is "+res_set_cass_size);
					sb_itemstorecleanup.delete(57, 120);
					item_no=items.readLine();
				}
				System.out.println("Total Execution Time :" +(System.currentTimeMillis() - startTime));
		  }
		  
		  finally{
				items.close();
				cass_conn.close();
		  }
		  */ 
		  
		  try{
				items = new BufferedReader(new FileReader("C:/EclipseData/TD_Items.txt"));
				item_no=items.readLine();
				res_set_curr_size=0;
				Statement st = null;
				while (item_no!=null)
				{	
					//select_itemstorecleanup="select  count(*)  from markdown.item_store_cleaned where item_nbr="+item_no+" and store_nbr<9000 ALLOW FILTERING";
					//res_set_cass=session.execute(select_itemstorecleanup);
					st = new SimpleStatement("select  count(*)  from markdown.item_store_cleaned;");
					st.setFetchSize(100);
					int count = 0;
					//System.out.println("For Item- "+item_no+", stores are "+res_set_cass.all());
					res_set_cass = session.execute(st);
					count = res_set_cass.all().size();
					while(!res_set_cass.isFullyFetched()){
						count = count +res_set_cass.all().size();
					}
					//item_no=items.readLine();
					System.out.println(count);
				}

		  }
		  catch(Exception e){
			  e.printStackTrace();
		  }
		  
		  finally{
				System.out.println("Total Execution Time :" +(System.currentTimeMillis() - startTime));
				items.close();
				cass_conn.close();
		  }

	}

}
