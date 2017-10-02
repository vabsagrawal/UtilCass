/**
 * 
 */
package com.vabs.validation;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
/**
 * @author v0b003r
 *
 */
public class sirCurrentInsertionMultiThread {
	private static final String FILENAME = System.getProperty("user.dir")+"/D721ItemsOnMD.txt";
	public static void main(String[] args) throws Exception{
		long starttime=System.currentTimeMillis();
		List<Thread> listThread = new ArrayList<>();
		System.out.println("Input File Path is "+FILENAME);
		BufferedReader br2=new BufferedReader(new FileReader(FILENAME));
		List<Integer> allLines = new ArrayList<>();
		String line;
		while ((line=br2.readLine()) != null) {allLines.add(Integer.parseInt(line));}
		br2.close();
		System.out.println("No of Items is "+allLines.size());
		int no_of_threads=Integer.parseInt(args[0]);
		int avg_count_per_thread= no_of_threads != 0 ? Math.round(allLines.size()/ no_of_threads) : allLines.size();
		System.out.println("Average Items per Thread is "+avg_count_per_thread);
		int count=0;
		int endInx = 0;
		int strtIndx = -1;
		int threadCount = 1;
		while (true && no_of_threads >0) {
			strtIndx = count;
			endInx = threadCount != no_of_threads && avg_count_per_thread > 0 ? count + avg_count_per_thread:count +(allLines.size() - strtIndx);
			System.out.println("Starting-Ending Value for Thread is "+strtIndx+"-"+endInx);
			sirInsertionThreadCode onboarding_thread= new sirInsertionThreadCode(new ArrayList<>(allLines.subList(strtIndx,  endInx)));
			listThread.add(onboarding_thread);
			onboarding_thread.start();
			if(endInx < allLines.size()){
			count += avg_count_per_thread;
			} else {
				break;
			}
			threadCount++;
		}	
		for (Thread t : listThread) {
			t.join();
		}
		System.out.println("Total time taken for Data Refresh is "+(System.currentTimeMillis()-starttime));
	}
}
