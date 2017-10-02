package com.vabs.validation;

import static com.nothing.utils.MarkdownConstants.cass_conn;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.nothing.cassandra.CassandraConnection;

public class CassandraSelect_LiftingMD {

	static Session session = null;
	static BufferedReader items,stores,dest_type,dest_id,plans;
	static ArrayList<String> plist,blist,ilist,slist;
	static PreparedStatement query,insert_sir;
	static String item,str2,str3;
	static ResultSet res_set_cass;
	static int store_size;
	static String destty,destid,item_no,store_no,plan_no;
	static String Select_MD_ITEM_StoreCount=null;
	public static void main(String[] args) throws Exception{
		  cass_conn=CassandraConnection.getCassConn_markdown();
		  BatchStatement batch = null;
		  session = cass_conn.getSess_markdown_perf();
		  try{
			  items = new BufferedReader(new FileReader("C:/EclipseData/LiftingMD_Items.txt"));
			  item_no=items.readLine();
					while (item_no!=null)
					{	
						Select_MD_ITEM_StoreCount="select count(*) from markdown.item_store_cleaned where item_nbr="+item_no+" and store_nbr<9000 ALLOW FILTERING;";
						res_set_cass=session.execute(Select_MD_ITEM_StoreCount);
						System.out.println("For Item -"+item_no+" count is "+res_set_cass.all());
						item_no=items.readLine();
						//Thread.sleep(10000);
					}

			  }
			  catch(Exception e){
				  e.printStackTrace();
			  }
			  
			  finally{
					items.close();
			  }
				

	}

}
