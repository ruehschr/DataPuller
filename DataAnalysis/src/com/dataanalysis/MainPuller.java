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
	private static final Exception QuitException = null;
	private static String BASE_URL = "http://api.crunchbase.com/v/1/company/%s.js?api_key=pxkyyhyq5f6em3pkxghhdzqy";
	static int idCounter;
	static int idCounterR;
	static int idCounterI;
	static int idCounterA;
	//static int idCounterPS;
	static Connection connection;
	static Connection connectionRounds;
	static Connection connectionInvestments;
	//static Connection connectionAcquisitions;
	//static Connection connectionPeople;
	static PreparedStatement ps;
	static PreparedStatement psR;
	static PreparedStatement psI;
	//static PreparedStatement psA;
	//static PreparedStatement psPS;
	static final int batchSize = 50;
	static final int batchSizeOther = 5;
	
	static final String sql = "insert into dbo.CrunchBase (ID, name, permalink, homepage_url, category_code, total_money_raised,status,country_code,state_code,city,funding_rounds,founded_at,founded_month,founded_year,first_funding_at,last_funding_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	static final String sqlRounds = "insert into dbo.Rounds (ID, permalink, name, category_code, country_code,state_code,city,funding_round_type,funded_at,funded_month, funded_year, raised_amount, raised_currency_code) values (?,?,?,?,?,?,?,?,?,?,?,?,?)";
	static final String sqlInvestments = "insert into dbo.Investments (ID, permalink, name,category_code,country_code,state_code,city,investor_permalink,investor_name,RoundsID,investor_type) values (?,?,?,?,?,?,?,?,?,?,?)";
	static final String sqlAcquisitions = "insert into dbo.Acquisitions (ID, permalink, name, category_code,country_code,state_code,city,acquired_permalink,acquired_name,acquired_at,acquired_month,acquired_year,price_amount,price_currency_code) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	//static final String sqlPeopleSmall = "insert into dbo.PeopleSmall (ID, firstname, lastname, permalink) values (?, ?, ?, ?)";
	
	public static void main(String[] args) throws SQLException {
		if(args.length == 4){
			idCounter = Integer.valueOf(args[0]);
			idCounterR = Integer.valueOf(args[1]);
			idCounterI = Integer.valueOf(args[2]);
			idCounterA = Integer.valueOf(args[3]);
		} else {
			idCounter = 30001;
			idCounterR = 5340;
			idCounterI = 3601;
			idCounterA = 637;
		}
		
		while(true) {
			System.out.println("Running ids " + idCounter + " - " + (idCounter + 5000));
			System.err.println("***********Running next shit!**********");
			runShit();
			
			try {
				System.out.println("Going to sleep");
				
			    Thread.sleep(300000);
			} catch(InterruptedException ex) {
			    Thread.currentThread().interrupt();
			}
		}
		
		 
	}

	private static void runShit() throws SQLException{
		//idCounterPS = 0;
				connection = DriverManager
						.getConnection("jdbc:sqlserver://fablrkg1ck.database.windows.net:1433;database=crunchbase-db;user=azureuser@fablrkg1ck;password={your_password_here};encrypt=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;","azureuser", "DataAnalysis1337");
				connectionRounds = DriverManager
						.getConnection("jdbc:sqlserver://fablrkg1ck.database.windows.net:1433;database=crunchbase-db;user=azureuser@fablrkg1ck;password={your_password_here};encrypt=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;","azureuser", "DataAnalysis1337");
				connectionInvestments = DriverManager
						.getConnection("jdbc:sqlserver://fablrkg1ck.database.windows.net:1433;database=crunchbase-db;user=azureuser@fablrkg1ck;password={your_password_here};encrypt=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;","azureuser", "DataAnalysis1337");
				//connectionAcquisitions = DriverManager
				//		.getConnection("jdbc:sqlserver://fablrkg1ck.database.windows.net:1433;database=crunchbase-db;user=azureuser@fablrkg1ck;password={your_password_here};encrypt=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;","azureuser", "DataAnalysis1337");
				//connectionPeople = DriverManager
				//		.getConnection("jdbc:sqlserver://fablrkg1ck.database.windows.net:1433;database=crunchbase-db;user=azureuser@fablrkg1ck;password={your_password_here};encrypt=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;","azureuser", "DataAnalysis1337");
				
					ps = connection.prepareStatement(sql);
					psR = connectionRounds.prepareStatement(sqlRounds);
					psI = connectionInvestments.prepareStatement(sqlInvestments);
					//psA = connectionAcquisitions.prepareStatement(sqlAcquisitions);
					//psPS = connectionPeople.prepareStatement(sqlPeopleSmall);
				try {
				JSONParser jsonParser = new JSONParser();
				
				JSONArray companyList = (JSONArray) jsonParser.parse(new FileReader("fullCompanyList.json"));
				
				
				
				for(int i = idCounter; i < idCounter + 5000; i++) {
					JSONObject companyPerma = (JSONObject) companyList.get(i);
					String response = apiRequest(String.format(BASE_URL, companyPerma.get("permalink")));
					if(response.compareTo("") != 0) {
						JSONObject jsonObject = (JSONObject) jsonParser.parse(response);
						System.out.println(jsonObject.get("permalink"));
						boolean error = addCompany(jsonObject);
						//boolean errorRI = addRoundsAndInvestors(jsonObject);
						
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
									System.out.println("###Batch loaded succesfully");
								}
					    	} catch(SQLServerException e) {
					    		e.printStackTrace();
					    		System.out.println("****Didn't upload companies " + (idCounter - 50) + " through " + idCounter);
					    	} catch(BatchUpdateException e) {
					    		e.printStackTrace();
					    		
					    		//System.err.println//
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
					if(e == QuitException){
						ps.close();
						connection.close();
						System.out.println("idCounter = " + idCounter);
						System.out.println("idCounterR = " + idCounterR);
						System.out.println("idCounterA = " + idCounterA);
						System.out.println("idCounterI = " + idCounterI);
						return;
					}
					
				}
				System.out.println("idCounter = " + idCounter);
				System.out.println("idCounterR = " + idCounterR);
				System.out.println("idCounterA = " + idCounterA);
				System.out.println("idCounterI = " + idCounterI);
				ps.close();
				connection.close();
		
	}

	private static boolean addCompany(JSONObject jobj) throws Exception {
		boolean error = false;
		try {
			ps.setInt(1, idCounter++);
			String name = (String)jobj.get("name");
			String permalink = (String)jobj.get("permalink");
			String homepage_url = (String)jobj.get("homepage_url");
			String category_code = (String)jobj.get("category_code");
			String state_code = null;
			String city = null;
			String country_code = null;
			ps.setString(2, name);
			ps.setString(3, permalink);
			ps.setString(4, homepage_url);
			ps.setString(5, category_code);
			if(name != null && name.length() > 50)
				System.err.println("Name is too long for " + permalink);
			if(permalink != null && permalink.length() > 50)
				System.err.println("permalink is too long for " + permalink);
			if(homepage_url != null && homepage_url.length() > 150)
				System.err.println("homepage_url is too long for " + permalink);
			if(category_code != null && category_code.length() > 50)
				System.err.println("Category is too long for " + permalink);

			JSONObject acqui = (JSONObject)jobj.get("acquisition");
			JSONObject ipo = (JSONObject)jobj.get("ipo");
			Long closed = (Long)jobj.get("deadpooled_year");
			
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
		        state_code = (String) p.get("state_code");
		        city = (String) p.get("city");
		        country_code = (String) p.get("country_code");
				ps.setString(8, country_code);
				ps.setString(9, state_code);
				ps.setString(10, city);   
		    } else {
		    	ps.setString(8, null);
		    	ps.setString(9, null);
		    	ps.setString(10, null);
		    }
		    
			float money_raised = 0f;
			Long firstDay = null, firstMonth = null, firstYear = null, lastDay = null, lastMonth = null, lastYear = null;
			
		    JSONArray rounds = (JSONArray) jobj.get("funding_rounds");
		    uploadRounds(rounds, permalink, name, category_code, country_code, state_code, city);
		    for(int i = 0; rounds != null && i < rounds.size(); ++i) {
		    	JSONObject p = (JSONObject)rounds.get(i);
		    	Double doub = (Double) p.get("raised_amount");	    	
		    	
		    	Long roundDay = (Long) p.get("funded_day");
		    	Long roundMonth = (Long) p.get("funded_month");
	    		Long roundYear = (Long) p.get("funded_year");	    		
	    		
		    	Float money;
		    	if(doub == null)
		    		money = 0f;
		    	else
		    		money = doub.floatValue();
		    	if(money != null)
		    		money_raised += money;
		    	if(i == 0) {
		    		firstDay = roundDay;
		    		firstMonth = roundMonth;
		    		firstYear = roundYear;
		    	}
		    	if(i == rounds.size()-1) {
		    		lastDay = roundDay;
		    		lastMonth = roundMonth;
		    		lastYear = roundYear;
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
			
			JSONArray acquisitions = (JSONArray) jobj.get("acquisitions");
			uploadAcquisitions(acquisitions, permalink, name, category_code, country_code, state_code, city);
			/*JSONArray relationships = (JSONArray) jobj.get("relationships");
			for(int rel = 0; relationships != null && rel < relationships.size(); ++rel) {
				JSONObject relObj = (JSONObject) relationships.get(rel);
				psPS.setInt(1, idCounterPS++);
				JSONObject personObj = (JSONObject) relObj.get("person");
				psPS.setString(2, (String) personObj.get("first_name"));
				psPS.setString(3, (String) personObj.get("last_name"));
				psPS.setString(4, (String) personObj.get("permalink"));
				psPS.addBatch();
				checkExecuteOnBatch(psPS, connectionPeople, sqlPeopleSmall, idCounterPS);
			}*/

		} catch (SQLServerException e) {
			e.printStackTrace();
			try {
				connection = DriverManager
						.getConnection("jdbc:sqlserver://fablrkg1ck.database.windows.net:1433;database=crunchbase-db;user=azureuser@fablrkg1ck;password={your_password_here};encrypt=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;","azureuser", "DataAnalysis1337");
			

			
				String sql = "insert into dbo.CrunchBase (ID, name, permalink, homepage_url, category_code, total_money_raised,status,country_code,state_code,city,funding_rounds,founded_at,founded_month,founded_year,first_funding_at,last_funding_at) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
				
				ps = connection.prepareStatement(sql);
			} catch (SQLException e1) {
				System.out.println("still can't connect. Exiting...");
				e1.printStackTrace();
				throw QuitException;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Error processing " + jobj.get("permalink"));
			error = true;
		}
		return error;
	}
	
	private static void uploadAcquisitions(JSONArray acquisitions,
			String permalink, String name, String category_code,
			String country_code, String state_code, String city) throws Exception {
		Connection connectionAcquisitions = DriverManager
				.getConnection("jdbc:sqlserver://fablrkg1ck.database.windows.net:1433;database=crunchbase-db;user=azureuser@fablrkg1ck;password={your_password_here};encrypt=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;","azureuser", "DataAnalysis1337");
		
		PreparedStatement psA = connectionAcquisitions.prepareStatement(sqlAcquisitions);
		for(int acq = 0; acquisitions != null && acq < acquisitions.size(); ++acq) {
			try {
				psA.setInt(1, idCounterA++);
		    	psA.setString(2, permalink);
		    	psA.setString(3, name);
		    	psA.setString(4, category_code);
		    	psA.setString(5, country_code);
		    	psA.setString(6, state_code);
		    	psA.setString(7, city);
		    	JSONObject acquired = (JSONObject) acquisitions.get(acq);
		    	JSONObject companyInfo = (JSONObject) acquired.get("company");
		    	if(companyInfo != null) {
		    		psA.setString(8, (String) companyInfo.get("permalink"));
		    		psA.setString(9, (String) companyInfo.get("name"));
		    	}
		    	Long acqDay = (Long) acquired.get("acquired_day");
		    	Long acqMonth = (Long) acquired.get("acquired_month");
		    	Long acqYear = (Long) acquired.get("acquired_year");
		    	if(acqYear != null && acqMonth != null && acqDay != null)
					psA.setString(10, String.format("%02d", acqMonth) + "/" + String.format("%02d", acqDay) + "/" + acqYear);
				else
					psA.setString(10, null);
		    	if(acqMonth == null) {
		    		psA.setNull(11, java.sql.Types.INTEGER);
		    	} else {
		    		psA.setInt(11, acqMonth.intValue());
		    	}
		    	if(acqYear == null) {
		    		psA.setNull(12, java.sql.Types.INTEGER);
		    	} else {
		    		psA.setInt(12, acqYear.intValue());
		    	}
		    	Double acqAmount = (Double) acquired.get("price_amount");
		    	
		    	if(acqAmount == null) {
		    		psA.setNull(13, java.sql.Types.FLOAT);
		    	} else {
		    		psA.setFloat(13, acqAmount.floatValue());
		    	}
		    	psA.setString(14, (String) acquired.get("price_currency_code"));
		    	psA.addBatch();
		    	
			} catch(SQLException e) {
				e.printStackTrace();
			}
			int[] returnArr1;
			try {
				returnArr1 = psA.executeBatch();
				// insert remaining records
				boolean error = false;
				for(int i = 0; i < returnArr1.length; i++) {
					if(returnArr1[i] != 1) {
						error = true;
					}
				}
				if(error) {
					System.out.println("there was an error uploading batch");
				} /*else {
					System.out.println("Batch loaded succesfully");
				}*/
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	}

	private static void uploadRounds(JSONArray rounds, String permalink,
			String name, String category_code, String country_code,
			String state_code, String city) throws Exception {
		for(int i = 0; rounds != null && i < rounds.size(); ++i) {
			JSONObject p = (JSONObject)rounds.get(i);
	    	try {
				psR.setInt(1, idCounterR++);
			
		    	psR.setString(2, permalink);
		    	psR.setString(3, name);
		    	psR.setString(4, category_code);
		    	psR.setString(5, country_code);
		    	psR.setString(6, state_code);
		    	psR.setString(7, city);
		    	
		    	
		    	Double doub = (Double) p.get("raised_amount");
		    	psR.setString(8, (String) p.get("round_code"));
		    	
		    	Long roundDay = (Long) p.get("funded_day");
		    	Long roundMonth = (Long) p.get("funded_month");
	    		Long roundYear = (Long) p.get("funded_year");
	    		if(roundDay != null && roundMonth != null && roundDay != null)
					psR.setString(9, String.format("%02d", roundMonth) + "/" + String.format("%02d", roundDay) + "/" + roundYear);
				else
					psR.setString(9, null);
	    		if(roundMonth == null) {
	    			psR.setNull(10,java.sql.Types.INTEGER);
	    		} else {
	    			psR.setInt(10, roundMonth.intValue());
	    		}
	    		if(roundYear == null) {
	    			psR.setNull(11,java.sql.Types.INTEGER);
	    		} else {
	    			psR.setInt(11, roundYear.intValue());
	    		}
	    		if(doub != null) {
	    			psR.setFloat(12, doub.floatValue());
	    		} else {
	    			psR.setNull(12, java.sql.Types.FLOAT);
	    		}
	    		psR.setString(13, (String) p.get("raised_currency_code"));
	    	
	    		
		    	psR.addBatch();
		    	checkExecuteOnBatch(psR, connectionRounds, sqlRounds, idCounterR, 1);
	    	
	    	} catch (SQLException e) {
				e.printStackTrace();
				try {
					connectionRounds = DriverManager
							.getConnection("jdbc:sqlserver://fablrkg1ck.database.windows.net:1433;database=crunchbase-db;user=azureuser@fablrkg1ck;password={your_password_here};encrypt=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;","azureuser", "DataAnalysis1337");
				
					
					psR = connectionRounds.prepareStatement(sqlRounds);
				} catch (SQLException e1) {
					System.out.println("still can't connect. Exiting...");
					e1.printStackTrace();
					throw QuitException;
				}
			}
	    	JSONArray investments = (JSONArray) p.get("investments");
	    	if(investments != null && investments.size() > 0) {
	    		connectionInvestments = DriverManager
						.getConnection("jdbc:sqlserver://fablrkg1ck.database.windows.net:1433;database=crunchbase-db;user=azureuser@fablrkg1ck;password={your_password_here};encrypt=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;","azureuser", "DataAnalysis1337");
			
				
				psI = connectionRounds.prepareStatement(sqlInvestments);
	    	}
	    	for(int iv = 0; investments != null && iv < investments.size();iv++) {
	    		try {
		    		JSONObject investObj = (JSONObject) investments.get(iv);
		    		psI.setInt(1, idCounterI++);
			    	psI.setString(2, permalink);
			    	psI.setString(3, name);
			    	psI.setString(4, category_code);
			    	psI.setString(5, country_code);
			    	psI.setString(6, state_code);
			    	psI.setString(7, city);
			    	JSONObject company = (JSONObject) investObj.get("company");
			    	JSONObject financial = (JSONObject) investObj.get("financial_org");
			    	JSONObject person = (JSONObject) investObj.get("person");
			    	if(person != null) {
			    		String newName = (String) person.get("first_name") + " " + (String) person.get("last_name");
			    		psI.setString(8, newName);
			    		psI.setString(9, (String) person.get("permalink"));
			    		psI.setString(11, "person");
			    	} else if(financial != null) {
			    		psI.setString(8, (String) financial.get("name"));
			    		psI.setString(9, (String) financial.get("permalink"));
			    		psI.setString(11, "financial_org");
			    	} else if(company != null) {
			    		psI.setString(8, (String) company.get("name"));
			    		psI.setString(9, (String) company.get("permalink"));
			    		psI.setString(11, "company");
			    	}
			    	psI.setInt(10, idCounterR-1);
			    	psI.addBatch();
			    	checkExecuteOnBatch(psI, connectionInvestments,sqlInvestments, idCounterI, 1);
	    		}catch (SQLException e) {
	    			e.printStackTrace();
	    			try {
						connectionInvestments = DriverManager
								.getConnection("jdbc:sqlserver://fablrkg1ck.database.windows.net:1433;database=crunchbase-db;user=azureuser@fablrkg1ck;password={your_password_here};encrypt=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;","azureuser", "DataAnalysis1337");
					
						
						psI = connectionRounds.prepareStatement(sqlInvestments);
					} catch (SQLException e1) {
						System.out.println("still can't connect. Exiting...");
						e1.printStackTrace();
						throw QuitException;
					}
	    		}
	    	}
	    }
		
	}

	private static void checkExecuteOnBatch(PreparedStatement executePS, Connection c, String sql, int counter, int batchSize) throws Exception {
		if(counter % batchSize != 0)
			return;
		int[] returnArr1;
		try {
			returnArr1 = executePS.executeBatch();
			// insert remaining records
			boolean error = false;
			for(int i = 0; i < returnArr1.length; i++) {
				if(returnArr1[i] != 1) {
					error = true;
				}
			}
			if(error) {
				System.out.println("there was an error uploading batch");
			}/* else {
				System.out.println("Batch loaded succesfully");
			}*/
		} catch (SQLException e) {
			try {
				c = DriverManager
						.getConnection("jdbc:sqlserver://fablrkg1ck.database.windows.net:1433;database=crunchbase-db;user=azureuser@fablrkg1ck;password={your_password_here};encrypt=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;","azureuser", "DataAnalysis1337");
				
				executePS = connection.prepareStatement(sql);
			} catch (SQLException e1) {
				System.out.println("still can't connect. Exiting...");
				e1.printStackTrace();
				throw QuitException;
			}
			e.printStackTrace();
		}
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