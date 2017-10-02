package com.vabs.validation;


import static com.nothing.utils.MarkdownConstants.cass_conn;
import static com.nothing.utils.MarkdownConstants.cass_conn_qa_sir;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import static com.nothing.utils.MarkdownConstants.cass_conn_prod_sir;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.nothing.cassandra.CassandraConnection;

public class CassandraDeletions_SIR {
	static Session session_qa = null,session_prod=null;
	static Session session = null;
	static BufferedReader items,stores,dest_type,dest_id,plans;
	static ArrayList<String> plist,blist,ilist,slist;
	static PreparedStatement query,insert_sir;
	static String item,str2,str3;
	static ResultSet res_set_cass;
	static int store_size;
	static String destty,destid,item_no,store_no,plan_no;
	static String delete_item=null;
	public static void main(String[] args) throws Exception
	{
		cass_conn_qa_sir=CassandraConnection.getCassConn_SIR();
		session_qa=cass_conn_qa_sir.getSess_StorePrices_SIR();
		  try{
			  items = new BufferedReader(new FileReader("C:/EclipseData/OnBoarding/D721ItemsOnMD.txt"));
			  item_no=items.readLine();
					while (item_no!=null)
					{	
						delete_item="DELETE FROM  storeprices.store_item_retail WHERE cntry_cd='US' AND div_nbr=1 AND item_nbr="+item_no+";";
						res_set_cass=session_qa.execute(delete_item);
						System.out.println("Item -"+item_no+" is deleted from SIR");
						item_no=items.readLine();
					}
					System.out.println("While Loop Ended");

			  }
			  catch(Exception e){
				  e.printStackTrace();
			  }
			  
			  finally{
					items.close();
					System.out.println("Inside Finally Block");
					session_qa.close();
			  }
	}

}
