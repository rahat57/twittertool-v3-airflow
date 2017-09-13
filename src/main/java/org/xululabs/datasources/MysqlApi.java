package org.xululabs.datasources;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import twitter4j.Twitter;

/**
 * Hello world!
 *
 */
public class MysqlApi {
	public static void main(String[] args) throws ClassNotFoundException,
			SQLException, IOException {

	}

	public String listToString(List<String> terms){
		String keyword = "";
		int count =0;
		for (int i = 0; i < terms.size(); i++) {
			
			if ( count == terms.size()-1 ) {

				keyword = keyword +	"("+terms.get(i)+")";
				}
			else {
				keyword = keyword +	"("+terms.get(i)+") OR ";
			}
	
			count++;
	
		} 
	
		return keyword.toString();
	}
	
	//FUNCTION TO CREATE DATABSE
	private static void createDb(String DbName) {
		final String DB_URL = "jdbc:mysql://localhost/";
		Connection conn = null;
		   Statement stmt = null;
		   try {
			// STEP 2: Open a connection   //metalchirper
				 conn = DriverManager.getConnection(DB_URL, "metalchirper", "metalchirper");
				System.out.println("creating database...");
			      stmt = conn.createStatement();
			      String sql = "CREATE DATABASE "+DbName;
			      stmt.executeUpdate(sql);
			      System.out.println("Database created successfully...");
		} catch (SQLException se) {
			se.printStackTrace();
		}
		   catch (Exception e) {
			   e.printStackTrace();
		}
		
	}

	
	public List<Integer> getUserIds(String dbName) throws ClassNotFoundException, SQLException {
		
		List<Integer> userids = new ArrayList<Integer>();
		
		Connection con = getDbConnection(dbName);
		if (con != null) {
			PreparedStatement preparedStatement = null;
			ResultSet result = null;
			String sql = "select * from user_info";
			try {
				preparedStatement = con.prepareStatement(sql);
				result = preparedStatement.executeQuery();
				while (result.next()) {
					userids.add(result.getInt("uid"));
				}

			} catch (Exception e) {
				System.err.println(e.getMessage());
			} finally {
				if (con != null) {
					con.close();
				}
			}
		}
		return userids;
	}
	
	public String getSearchterms(String dbName,int suid) throws ClassNotFoundException, SQLException {
		
		List<String> queryKeywords = new ArrayList<String>();
		String keywords = "";
		Connection con = getDbConnection(dbName);
		if (con != null) {
			PreparedStatement preparedStatement = null;
			ResultSet result = null;
			String sql = "select searchstring from tt_search_jobs where suid = ?";
			try {
				preparedStatement = con.prepareStatement(sql);
				preparedStatement.setInt(1, suid);
				result = preparedStatement.executeQuery();
				while (result.next()) {
					
					queryKeywords.add(result.getString("searchstring"));
	
				}
				keywords = this.listToString(queryKeywords);
				
			} catch (Exception e) {
				System.err.println(e.getMessage());
			} finally {
				if (con != null) {
					con.close();
				}
			}
		}
		return keywords;
	}

	public String getIndexEncryption(String dbName,int suid) throws ClassNotFoundException, SQLException {
		
		String queryKeywords ="";
		Connection con = getDbConnection(dbName);
		if (con != null) {
			PreparedStatement preparedStatement = null;
			ResultSet result = null;
			String sql = "select encrypted from user_info where uid = ?";
			try {
				preparedStatement = con.prepareStatement(sql);
				preparedStatement.setInt(1, suid);
				result = preparedStatement.executeQuery();
				while (result.next()) {
					
					queryKeywords = result.getString("encrypted");
	
				}

			} catch (Exception e) {
				System.err.println(e.getMessage());
			} finally {
				if (con != null) {
					con.close();
				}
			}
		}
		return queryKeywords;
	}
	
	public Map<String, Object> getUidAndIndexEncryption(String dbName,String screenName) throws ClassNotFoundException, SQLException {
		
		Map<String, Object> uidIndexEncrypted =new HashMap<String, Object>();
		Connection con = getDbConnection(dbName);
		if (con != null) {
			PreparedStatement preparedStatement = null;
			ResultSet result = null;
			String sql = "select user_id,encrypted,uid from user_info where screen_name = ?";
			try {
				preparedStatement = con.prepareStatement(sql);
				preparedStatement.setString(1, screenName);
				result = preparedStatement.executeQuery();
				while (result.next()) {
					
					uidIndexEncrypted.put("uid", result.getString("user_id"));
					uidIndexEncrypted.put("encrypted", result.getString("encrypted"));
					uidIndexEncrypted.put("userId", result.getString("uid"));
	
				}

			} catch (Exception e) {
				System.err.println(e.getMessage());
			} finally {
				if (con != null) {
					con.close();
				}
			}
		}
		return uidIndexEncrypted;
	}
	
	public  Map<String, Object> getAuthKeys(String dbName) throws ClassNotFoundException, SQLException {
		
		Map<String,Object> queryKeywords = new HashMap<String,Object>();
		Connection con = getDbConnection(dbName);
		if (con != null) {
			PreparedStatement preparedStatement = null;
			ResultSet result = null;
			String sql = "select * from tt_twitter_app ORDER BY last_used LIMIT 1";
			try {
				preparedStatement = con.prepareStatement(sql);

				result = preparedStatement.executeQuery();
				while (result.next()) {
//					System.err.println(result.getString("consumerKey"));
					queryKeywords.put("consumerKey",result.getString("consumerKey").trim());
					queryKeywords.put("consumerSecret",result.getString("consumerSecret").trim());
					queryKeywords.put("accessToken",result.getString("accessToken").trim());
					queryKeywords.put("accessTokenSecret",result.getString("accessTokenSecret").trim());
	
				}

			} catch (Exception e) {
				System.err.println(e.getMessage());
			} finally {
				if (con != null) {
					con.close();
				}
			}
		}
		return queryKeywords;
	}

public Map<String, Object> getAuthKeysByUid(String dbName,String uid) throws ClassNotFoundException, SQLException {
		
		Map<String,Object> queryKeywords = new HashMap<String,Object>();
		Connection con = getDbConnection(dbName);
		if (con != null) {
			PreparedStatement preparedStatement = null;
			ResultSet result = null;
			String sql = "select * from tt_twitter_app where uid = ?";
			try {
				preparedStatement = con.prepareStatement(sql);
				preparedStatement.setString(1, uid);
				result = preparedStatement.executeQuery();
				while (result.next()) {
					
					queryKeywords.put("consumerKey",result.getString("consumerKey").trim());
					queryKeywords.put("consumerSecret",result.getString("consumerSecret").trim());
					queryKeywords.put("accessToken",result.getString("accessToken").trim());
					queryKeywords.put("accessTokenSecret",result.getString("accessTokenSecret").trim());
	
				}

			} catch (Exception e) {
				
				System.err.println("getting issue for uid "+uid+" "+e.getMessage());
			
			} finally {
				if (con != null) {
					con.close();
				}
			}
		}
		return queryKeywords;
	}

public List<String> getAllUids(String dbName) throws ClassNotFoundException, SQLException {
	
	List<String> allUids = new ArrayList<String>();
	Connection con = getDbConnection(dbName);
	if (con != null) {
		PreparedStatement preparedStatement = null;
		ResultSet result = null;
		String sql = "select * from tt_twitter_app";
		try {
			preparedStatement = con.prepareStatement(sql);
			result = preparedStatement.executeQuery();
			while (result.next()) {
				
				allUids.add(result.getString("uid").trim());
			}

		} catch (Exception e) {
			
			System.err.println("getting issue for uid "+" "+e.getMessage());
		
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}
	return allUids;
}

//get all screenNames
public List<String> getAllScreenNames(String dbName) throws ClassNotFoundException, SQLException {
	
	List<String> allScreenNames = new ArrayList<String>();
	Connection con = getDbConnection(dbName);
	if (con != null) {
		PreparedStatement preparedStatement = null;
		ResultSet result = null;
		String sql = "select * from user_info";
		try {
			preparedStatement = con.prepareStatement(sql);
			result = preparedStatement.executeQuery();
			while (result.next()) {
				
				allScreenNames.add(result.getString("screen_name").trim());
				
			}

		} catch (Exception e) {
			
			System.err.println("getting issue for uid "+" "+e.getMessage());
		
		} finally {
			if (con != null) {
				con.close();
			}
		}
	}
	return allScreenNames;
}

	public  void updateTimeStamp(String dbName,String consumerKey) throws ClassNotFoundException, SQLException {
		
		Connection con = getDbConnection(dbName);
		if (con != null) {
			PreparedStatement preparedStatement = null;
			String sqlUpdateTimeStasmp = "UPDATE tt_twitter_app SET last_used = ? "
	                  + " WHERE consumerKey = ?";
			Timestamp timestamp = new Timestamp(System.currentTimeMillis());
			try {
				
				preparedStatement = con.prepareStatement(sqlUpdateTimeStasmp);
				preparedStatement.setObject(1, timestamp);
				preparedStatement.setString(2, consumerKey);
				int query = preparedStatement.executeUpdate();
				
				if (query > 0) {
					System.out.println("Authkeys timeStamp updated Successfully ..!");
				}

			} catch (Exception e) {
				System.err.println(e.getMessage());
			} finally {
				if (con != null) {
					con.close();
				}
			}
		}
	}

	private static Connection getDbConnection(String dbName) throws ClassNotFoundException,
			SQLException {

		final String DB_URL = "jdbc:mysql://localhost/"+dbName;

		// STEP 2: Open a connection
		System.out.println("Connecting to database..."); //dbName "root",""
		Connection conn = DriverManager.getConnection(DB_URL, dbName,dbName);
		return conn;
	}

	// Function to LOAD FILE DATA
	public static BufferedReader loadFile(String file)throws FileNotFoundException {

		// for reading files from root dorectory
		InputStream in = new FileInputStream(new File(file));
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		return reader;
	}
}
