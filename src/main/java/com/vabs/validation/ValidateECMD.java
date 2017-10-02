package com.vabs.validation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import com.nothing.cassandra.*;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class ValidateECMD {

	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		CassandraConnection connect_sir = null;
		CassandraConnection connect_perf=null;
		CassandraConnection connect_cert=null;
		
		Session sess_sir = null; 
		Session sess_perf = null; 
		Session sess_cert=null;
		
		PreparedStatement prepared_select_items=null;
		PreparedStatement preparedCountStores=null;
		PreparedStatement preparedSelectScenariostatus=null; 
		PreparedStatement preparedSelectScenarioDetails=null;
		
		BufferedReader scenario_ids=null;
		BufferedReader stores=null;
	
		String scenario_id;
		
		
		//List<Integer> itemsList = new ArrayList<>();
		List<Integer> storesList = new ArrayList<>();
		List<String> scenariosList = new ArrayList<>();
		
		Map<String,Set<Integer>> scenarioItemsMap = new HashMap<>();
		
		Set<Integer> itemsSet = new HashSet<>();
		
		String SELECT_ITEMS = "select items_set from storeprices.md_scenarios_aggr where scenario_id=?";
		String COUNT_STORES = "select count(*) from storeprices.markdown_price_change where scenario_id=? and item_nbr=?"; 
		
		String SELECT_SCENARIOSTATUS = "SELECT scenario_id, creator_id, item_cnt, lst_chg_ts, status, \"type\" FROM storeprices.scenario_status WHERE scenario_id=?";
		String SELECT_SCENARIODETAILS = "SELECT count(*) FROM storeprices.scenario_status_details WHERE scenario_id=?";
		
		try {
			
			connect_sir = CassandraConnection.getCassConn_SIR();
			sess_sir=connect_sir.getSess_StorePrices_SIR();
			
			/*connect_perf = CassandraConnection.getCassConn_markdown();
			sess_perf = connect_perf.getSess_markdown_perf();*/
			
			connect_cert = CassandraConnection.getCassConn_pricechange();
			sess_cert=connect_cert.getSess_StorePrice_cert();
			
			scenario_ids = new BufferedReader(new FileReader("C:/Users/vagraw1/Documents/Utilities/Scenarios.txt"));
			stores = new BufferedReader(new FileReader("C:/Users/vagraw1/Documents/Utilities/Stores.txt")); 
			String store;
			
			while((store=stores.readLine())!=null){
				
				storesList.add(Integer.parseInt(store));
				
			}
				
			while((scenario_id=scenario_ids.readLine())!=null){
				
				scenariosList.add(scenario_id);
				
			}
			
			System.out.println("Number of Stores : "+storesList.size());
			
			prepared_select_items = sess_cert.prepare(SELECT_ITEMS).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);	
			preparedCountStores = sess_cert.prepare(COUNT_STORES).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
			
			preparedSelectScenariostatus = sess_sir.prepare(SELECT_SCENARIOSTATUS).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);	
			preparedSelectScenarioDetails = sess_sir.prepare(SELECT_SCENARIODETAILS).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);	
			
			for (String scenario : scenariosList){
				
				ResultSet rsSelectItems = sess_cert.execute(prepared_select_items.bind(UUID.fromString(scenario)));
				Row row = rsSelectItems.one();
				
				itemsSet = row.getSet("items_set", Integer.class);
				System.out.println("Item Set for Scenario ID "+scenario);
				
				scenarioItemsMap.put(scenario, itemsSet);
				
				for (Integer item : itemsSet){
					
					ResultSet rsCountStores = sess_cert.execute(preparedCountStores.bind(UUID.fromString(scenario),new Integer(item)));
					Row rowCount = rsCountStores.one();
					
					long storeCount = rowCount.getLong(0);
					System.out.println("Number of Stores for Scenario ID # : "+scenario+" and Item # : "+item+"is : "+storeCount);
					
				}
				
				ResultSet rsScenarioStatus = sess_cert.execute(prepared_select_items.bind(UUID.fromString(scenario)));
				Row rowScenarioStatus = rsScenarioStatus.one();
				
				ScenarioStatus scenarioStatus = new ScenarioStatus();
				scenarioStatus.setScenarioId(rowScenarioStatus.getUUID(0));
				scenarioStatus.setCreatorId(rowScenarioStatus.getString(1));
				
				
				
				ResultSet rsScenarioDetails = sess_cert.execute(prepared_select_items.bind(UUID.fromString(scenario)));
				Row rowScenarioDetails = rsScenarioDetails.one();
				
				
				
			}
			
						
			System.out.println("Total Items in MD Scenario Aggregation Table = "+ "for Scenario ID ");
			System.out.println(scenarioItemsMap.get(scenariosList.get(0)));
			System.out.println(scenarioItemsMap.get(scenariosList.get(1)));
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			
			sess_cert.close();
			connect_cert.close();
					
		}
				
	}

}
