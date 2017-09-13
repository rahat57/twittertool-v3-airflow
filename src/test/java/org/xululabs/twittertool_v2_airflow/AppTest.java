package org.xululabs.twittertool_v2_airflow;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.xululabs.datasources.MysqlApi;
import org.xululabs.datasources.UtilFunctions;

/**
 * Unit test for simple MysqlApi.
 */
public class AppTest 
    extends TestCase
{
 static MysqlApi mysql = new MysqlApi();
    public static void main(String args[]) throws IOException, InterruptedException{
    	
    	int cnt =0;
    	for (int i = 0; i < 7; i++) {
    		
			cnt =cnt+100;
			
			if (cnt==500 || i ==6 && cnt < 500) {
				System.err.println(cnt);
				cnt =0;
			}
		}
    	
    	/*UtilFunctions fun = new UtilFunctions();
    	LinkedList<ArrayList<Map<String, Object>>> profilesBulks = new LinkedList<ArrayList<Map<String, Object>>>();
    	profilesBulks =	 fun.loadProfilesData("C://Users//xululabs//Desktop//twittertool-airflow//metalchirper1//datafiles//ffm_profilesData//sidrasheikh_=commonRelation");
    	
    	System.err.println(profilesBulks.size());
    	for (ArrayList<Map<String, Object>> map : profilesBulks) {
    		System.err.println(map.size());
    		for (Map<String, Object> map2 : map) {
//    			System.err.println(""+map2.get("id"));
			}
    		
		}
    	*/
    	
    	
    	/*while (true) {
    		
//    		Thread.sleep((long)(Math.random() * 600000));
    		
    		Random random = new Random();
    		long min = 60000;
    		long max = 600000;
//    		long randomNumber = random.nextLong((max + 1 - min) + min);
    		int randomNumber = random.nextInt(10 + 1 - 1) + 1;
//        	System.err.println(randomNumber);
        	
//        	System.err.println((1* 60 * 1000));
        	
        	long millis = TimeUnit.MINUTES.toMillis(randomNumber);
//        	System.err.println(millis);
        	
        	long LOWER_RANGE =60000; //assign lower range value
        	 long UPPER_RANGE = 600000; //assign upper range value

        	 long randomValue = LOWER_RANGE +(long)(random.nextDouble()*(UPPER_RANGE - LOWER_RANGE));
        	 
        	 System.err.println(randomValue);
        		
		}*/
    	
//    	String path = "C:/Users/xululabs/Desktop/twittertool-airflow/metalchirper/datafiles/terms";
//    	File folder = new File(path);
//    	File[] listOfFiles = folder.listFiles();
//    		List<String> fileNames =  new ArrayList<String>();
//    	    for (int i = 0; i < listOfFiles.length; i++) {
//    	      if (listOfFiles[i].isFile()) {
//    	    	  System.out.println("File " + listOfFiles[i].getName());
//    	    	  fileNames.add(listOfFiles[i].getName());
//    	    	
//    	      } else if (listOfFiles[i].isDirectory()) {
//    	        System.out.println("Directory " + listOfFiles[i].getName());
//    	      }
//    	    }
////    	    String name[] = names.split("_");
//    	    for (int i = 0; i < fileNames.size(); i++) {
//    	    	String name[] = fileNames.get(i).split("_");
//				System.err.println(name[0]+" terms "+loadFile(path+fileNames.get(i)));
//			}
//    	System.err.println("id "+name[0]);
    	    
    }
    
    
 // Function to LOAD FILE DATA
  	public static String loadFile(String file)	throws IOException {
  		List<String> searchJobs = new ArrayList<String>();
  		// CSVReader reader1 = new CSVReader(new FileReader("yourfile.csv"));
  		InputStream in = new FileInputStream(new File(file));
  		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
  		String line;
 		while ((line = reader.readLine()) != null) {
 			searchJobs.add(line);
 		}
 		reader.close();
 		String keywords = mysql.listToString(searchJobs);
 	return keywords;
  	}
    
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }
    
    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        assertTrue( true );
    }
}
