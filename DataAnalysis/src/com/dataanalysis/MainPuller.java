package com.dataanalysis;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;

import com.microsoft.sqlserver.jdbc.SQLServerException;

public class MainPuller {
	private final static String USER_AGENT = "Mozilla/5.0";
	private static String BASE_URL = "http://api.crunchbase.com/v/1/company/%s.js?api_key=pxkyyhyq5f6em3pkxghhdzqy";
	static int idCounter;
	
	public static void main(String[] args) throws SQLException {
		idCounter = 0;
		Connection connection = DriverManager
				.getConnection("jdbc:sqlserver://fablrkg1ck.database.windows.net:1433;database=crunchbase-db;user=azureuser@fablrkg1ck;password={your_password_here};encrypt=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;","azureuser", "DataAnalysis1337");

		
			String sql = "insert into dbo.CrunchBase (ID, name, permalink, homepage_url, category_code, total_money_raised,status,country_code,state_code,city,funding_rounds,founded_at,founded_month,founded_year,first_funding_at,last_funding_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			
			PreparedStatement ps = connection.prepareStatement(sql);
		try {
		JSONParser jsonParser = new JSONParser();
		
		JSONArray companyList = (JSONArray) jsonParser.parse(new FileReader("fullCompanyList.json"));
		
		 

		
		
		final int batchSize = 50;
		
		for(int i = 0; i < 30000; i++) {
			JSONObject companyPerma = (JSONObject) companyList.get(i);
			String response = apiRequest(String.format(BASE_URL, companyPerma.get("permalink")));
			if(response.compareTo("") != 0) {
				JSONObject jsonObject = (JSONObject) jsonParser.parse(response);
				System.out.println(jsonObject.get("permalink"));
				boolean error = addCompany(jsonObject, ps);
				
			    if(!error) {
			    	ps.addBatch();
			    } else {
			    	System.out.println("Error occurred. Leaving out of batch...");
			    }
			    if((1 + i) % batchSize == 0) { 
			    	try {
				        int [] returnArr = ps.executeBatch();
						boolean exe_error = false;
						for(int j = 0; j < returnArr.length; j++) {
							if(returnArr[j] != 1) {
								exe_error = true;
							}
						}
						if(exe_error) {
							System.out.println("there was an error uploading batch");
						} else {
							System.out.println("Batch loaded succesfully");
						}
			    	} catch(SQLServerException e) {
			    		e.printStackTrace();
			    		System.out.println("****Didn't upload companies " + (idCounter - 50) + " through " + idCounter);
			    	}
			    }
			}
		    
		}
		int [] returnArr1 = ps.executeBatch(); // insert remaining records
		boolean error = false;
		for(int i = 0; i < returnArr1.length; i++) {
			if(returnArr1[i] != 1) {
				error = true;
			}
		}
		if(error) {
			System.out.println("there was an error uploading batch");
		} else {
			System.out.println("Batch loaded succesfully");
		}
		} catch(Exception e) {
			e.printStackTrace();
			
		}
		ps.close();
		connection.close();
		 
	}
	
	private static boolean addCompany(JSONObject jobj, PreparedStatement ps ) {
		boolean error = false;
		try {
			ps.setInt(1, idCounter++);
			ps.setString(2, (String)jobj.get("name"));
			ps.setString(3, (String)jobj.get("permalink"));
			ps.setString(4, (String)jobj.get("homepage_url"));
			ps.setString(5, (String)jobj.get("category_code"));
			
			JSONObject acqui = (JSONObject)jobj.get("acquisition");
			JSONObject ipo = (JSONObject)jobj.get("ipo");
			String closed = (String)jobj.get("deadpooled_year");
			
			if(acqui != null)
				ps.setString(7, "acquired");
			else if(ipo != null)
				ps.setString(7, "ipo");
			else if(closed != null)
				ps.setString(7, "closed");
			else
				ps.setString(7, "operating");
			
			JSONArray offices = (JSONArray) jobj.get("offices");
		    if(offices.size() > 0){
		        JSONObject p = (JSONObject)offices.get(0);

				ps.setString(8, (String) p.get("country_code"));
				ps.setString(9, (String) p.get("state_code"));
				ps.setString(10, (String) p.get("city"));   
		    } else {
		    	ps.setString(8, null);
		    	ps.setString(9, null);
		    	ps.setString(10, null);
		    }
		    
			float money_raised = 0f;
			Long firstDay = null, firstMonth = null, firstYear = null, lastDay = null, lastMonth = null, lastYear = null;
		    JSONArray rounds = (JSONArray) jobj.get("funding_rounds");
		    for(int i = 0; i < rounds.size(); ++i) {
		    	JSONObject p = (JSONObject)rounds.get(i);
		    	Double doub = (Double) p.get("raised_amount");
		    	Float money;
		    	if(doub == null)
		    		money = 0f;
		    	else
		    		money = doub.floatValue();
		    	if(money != null)
		    		money_raised += money;
		    	if(i == 0) {
		    		firstDay = (Long) p.get("funded_day");
		    		firstMonth = (Long) p.get("funded_month");
		    		firstYear = (Long) p.get("funded_year");
		    	}
		    	if(i == rounds.size()-1) {
		    		lastDay = (Long) p.get("funded_day");
		    		lastMonth = (Long) p.get("funded_month");
		    		lastYear = (Long) p.get("funded_year");
		    	}
		    }
			ps.setFloat (6, money_raised);
			
			ps.setInt(11, rounds.size());
			
			Long year = (Long)jobj.get("founded_year");
			Long month = (Long)jobj.get("founded_month");
			if(month != null)
				ps.setInt(13, month.intValue());
			else
				ps.setNull(13, java.sql.Types.INTEGER);
			
			if(year != null)
				ps.setInt(14, year.intValue());
			else
				ps.setNull(14, java.sql.Types.INTEGER);
			
			Long day = (Long)jobj.get("founded_day");
			if(year != null && month != null && day != null)
				ps.setString(12, String.format("%02d", month) + "/" + String.format("%02d", day) + "/" + year);
			else
				ps.setString(12, null);
			
			if(firstDay != null && firstMonth != null && firstYear != null)
				ps.setString(15, String.format("%02d", firstMonth) + "/" + String.format("%02d", firstDay) + "/" + firstYear);
			else
				ps.setString(15, null);
			
			if(lastDay != null && lastMonth != null && lastYear != null)
				ps.setString(16, String.format("%02d", lastMonth) + "/" + String.format("%02d", lastDay) + "/" + lastYear);
			else
				ps.setString(16, null);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Error processing " + jobj.get("permalink"));
			error = true;
		}
		return error;
	}
	
	private static String apiRequest (String url) throws Exception {
		StringBuffer response;
		try {
		URL obj = new URL(url);
		HttpURLConnection connection = (HttpURLConnection) obj.openConnection();
		connection.setRequestMethod("GET");
 
		//add request header
		connection.setRequestProperty("User-Agent", USER_AGENT);
 
		BufferedReader in = new BufferedReader(
		        new InputStreamReader(connection.getInputStream()));
		String inputLine;
		response = new StringBuffer();
 
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
		} catch(Exception e) {
			e.printStackTrace();
			return "";
		}
		return response.toString();
		
	}
}