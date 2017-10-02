package com.vabs.validation;
import static com.nothing.utils.MarkdownConstants.mysql_conn;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.ResultSet;
import java.sql.Statement;

import com.datastax.driver.core.PreparedStatement;
import com.nothing.mysql.MysqlConnection;
public class MysqlInsertions {
	static int acctg_dept_nbr,dept_nbr,dept_cat_nbr;
	static String item_no;
	static String planid,sql1;
	static PreparedStatement query,insert_sir;
	static BufferedReader planID;
	static BufferedReader items;
	static Statement st=null;
	static ResultSet rs=null;
	static int res_type;
	String buyer_uid;
	static String insert_MBMPlanBuyerDetail=null;
	static String insert_pilotitems="insert into MdPlans.pilot_items (item_nbr) values(?)";
	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception{
		//mysql_conn=MysqlConnection.mysqlConnection();
		//st=mysql_conn.createStatement();
		StringBuilder str = new StringBuilder("insert into MdPlans.pilot_dept values(");
		items = new BufferedReader(new FileReader("C:/PilotDept.txt"));
		while ((item_no=items.readLine())!=null)
		{
			try{
			//sql1=String.format("insert into MdPlans.pilot_items values (%s)",item_no);
			//res_type=st.executeUpdate(sql1);
			str.append(item_no).append("),(");
			}
			catch(Exception e){
				System.out.println(e);
			}
					
		}
		System.out.println(str.toString());
	}

}
