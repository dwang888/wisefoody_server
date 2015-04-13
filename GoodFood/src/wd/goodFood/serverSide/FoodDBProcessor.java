package wd.goodFood.serverSide;
//just for caching purpose; and one time run
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.io.*;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

import wd.goodFood.entity.Business;
import wd.goodFood.entity.Food;
import wd.goodFood.entity.Review;
import wd.goodFood.nlp.GoodFoodFinder;
import wd.goodFood.utils.Configuration;
import wd.goodFood.utils.DBConnector;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

public class FoodDBProcessor{
	private Configuration config;
	private static int numBusiness;//how many business results should be returned per request.
	private String apiPrefixPlace;	
	//for fetcheding detail for each place, but only 3 reviews are returned for each place
	private String apiPrefixReview;	
	private String apiSurfixReview;	
	private JsonParser jsonParser;//should be thread safe
	private GoodFoodFinder finder;
	Connection dbconn;

	
	String SELECT_biz = "SELECT * FROM goodfoodDB.goodfood_biz_FourSquare bizSrcID = ?";
		
	String SELECT_reviews = "SELECT id,text,rLink,taggedText,food,dataSource,insertTime FROM goodfoodDB.goodfood_reviews_FourSquare WHERE bizSrcID = ? AND dataSource = ?";
	String INSERT_foods = "INSERT INTO goodfood_food_FourSquare (foodText,bizSrcID,dataSource,bizName,address,latitude,longitude,phoneNum,bizWebsite,id) VALUES (?,?,?,?,?,?,?,?,?,?)";
	String INSERT_Food2Review = "INSERT INTO gf_Food2Review_FourSquare (FoodID, ReviewID) VALUES (?,?)";
	
	
	PreparedStatement psSelectReviews = null;
	PreparedStatement psInsertFoods = null;
	PreparedStatement psInsertFood2Review = null;//many to many relationship
	
	
	
	
	public FoodDBProcessor(){
		this.setJsonParser(new JsonParser());
//		System.out.println("initilize BizDBProcessor");
		String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
		String DB_URL = "jdbc:mysql://192.241.173.181:3306/goodfoodDB";
		String USER = "lingandcs";
		String PASS = "sduonline";
		try {
			Class.forName("com.mysql.jdbc.Driver");
			this.dbconn = DriverManager.getConnection(DB_URL,USER,PASS);
			System.out.println("Connecting to database is built!");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			this.psSelectReviews = this.dbconn.prepareStatement(this.SELECT_reviews);
			this.psInsertFoods = this.dbconn.prepareStatement(this.INSERT_foods);
			this.psInsertFood2Review = this.dbconn.prepareStatement(this.INSERT_Food2Review);			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public List<Business> fetchPlacesFromDB(String bizDB, String bizID){
		//use json here; an xml based version may be needed
//		System.out.println(lat);
		String select_biz = "SELECT * FROM goodfoodDB." + bizDB + " WHERE bizSrcID = " + "'" + bizID + "'";
//		String select_biz = "SELECT * FROM goodfoodDB.goodfood_biz" + " WHERE bizSrcID = 758653";
		
//		System.out.println("SQL query:\t" + select_biz);
		long startTime = System.currentTimeMillis();
		PreparedStatement psSelectBiz = null;
		
		List<Business> bizs = new ArrayList<Business>();
		Business  biz = new Business();
//		System.out.println("fetching PLACEs from:\t");
//		System.out.println(apiStr);		
		try {

			Statement stmt = null;
			
			stmt = this.dbconn.createStatement();
			
//			ResultSet rs2 = stmt.executeQuery("SELECT * FROM goodfoodDB.goodfood_biz_FourSquare WHERE bizSrcID = '4accf7c9f964a5204dca20e3'");
	
			
			ResultSet rs = stmt.executeQuery(select_biz);
			while(rs.next()){
				String bizName = rs.getString("bizName");
				String address = rs.getString("address");
				String latitude = rs.getString("latitude");
				String longitude = rs.getString("longitude");
				String phoneNum = rs.getString("phoneNum");
				String merchantMsg = rs.getString("merchantMsg");
				String offer = rs.getString("offer");
				int numReviews = rs.getInt("numReviews");
				String profileLink = rs.getString("profileLink");
				String bizWebsite = rs.getString("bizWebsite");
				int dataSource = rs.getInt("dataSource");
				String updateTime = rs.getString("updateTime");
//				System.out.println(bizName + address);
				
				biz.setBusiness_name(bizName);
				biz.setBusiness_address(address);
				biz.setLatitude(latitude);
				biz.setLongitude(longitude);
				biz.setBusiness_phone(phoneNum);
				biz.setBusiness_id(bizID);
				biz.setDataSource(dataSource);
				biz.setBusiness_merchantMsg(merchantMsg);
				biz.setBusiness_offer(offer);
				biz.setNumReviews(numReviews);
				biz.setWebsite(bizWebsite);		
				
				bizs.add(biz);
//				break;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DBConnector.close(dbconn);
			DBConnector.close(psSelectBiz);
			
		}		
		
		long endTime = System.currentTimeMillis();
//		System.out.println("time fetch PLACES from API:\t" + (endTime - startTime));
		return bizs;
	}
	
	public List<Business> fetchPlacesFromDB(String bizDB){
		//use json here; an xml based version may be needed
//		String select_biz = "SELECT * FROM goodfoodDB." + bizDB;
		
//		System.out.println("SQL query:\t" + select_biz);
		long startTime = System.currentTimeMillis();
		Connection dbconn = null;
		PreparedStatement psSelectBiz = null;
		
		List<Business> bizs = new ArrayList<Business>();
		
		try {
//			String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
//			String DB_URL = "jdbc:mysql://192.241.173.181:3306/goodfoodDB";
//			String USER = "lingandcs";
//			String PASS = "sduonline";
//			Class.forName("com.mysql.jdbc.Driver");
//			System.out.println("Connecting to database");
//			dbconn = DriverManager.getConnection(DB_URL,USER,PASS);
			dbconn = this.dbconn;
			
			Statement stmt = null;
			
			stmt = dbconn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT bizName,bizSrcID,address,latitude,longitude,phoneNum,bizWebsite,dataSource FROM goodfoodDB." + bizDB);
			System.out.println("outputting result...");
			while(rs.next()){
				String bizName = rs.getString("bizName");
				String bizSrcID = rs.getString("bizSrcID");
				String address = rs.getString("address");
				String latitude = rs.getString("latitude");
				String longitude = rs.getString("longitude");
				String phoneNum = rs.getString("phoneNum");
				String bizWebsite = rs.getString("bizWebsite");
				int dataSource = rs.getInt("dataSource");
//				System.out.println(bizSrcID);
				
				Business  biz = new Business();
				biz.setBusiness_name(bizName);
				biz.setBusiness_address(address);
				biz.setLatitude(latitude);
				biz.setLongitude(longitude);
				biz.setBusiness_phone(phoneNum);
				biz.setBusiness_id(bizSrcID);
				biz.setDataSource(dataSource);
				biz.setWebsite(bizWebsite);
				
				bizs.add(biz);
//				break;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
//			DBConnector.close(dbconn);
			DBConnector.close(psSelectBiz);
			
		}		
		
		long endTime = System.currentTimeMillis();
		System.out.println("time fetch PLACES from API:\t" + (endTime - startTime));
//		System.out.println(bizs.size() + "\t biz fetched");
		return bizs;
	}

	public Business fetchReviewsFromDB(Business biz, Connection conn, PreparedStatement ps){
		int rID;
		String rStr;
		String rLink;
		String NEStr;
		String taggedText;
		ResultSet rs = null;
		
		try {
			ps.setString(1, biz.getBusiness_id());
			ps.setInt(2, biz.getDataSource());//data source TODO: hardcode?
			rs = ps.executeQuery();
			
//			Statement stmt = conn.createStatement();
//			String queryStr = "SELECT * FROM goodfoodDB.goodfood_reviews_FourSquare WHERE bizSrcID = \'" + biz.getBusiness_id() + "\'";
//			rs = stmt.executeQuery("SELECT * FROM goodfoodDB.goodfood_reviews_FourSquare WHERE bizSrcID = \'" + biz.getBusiness_id() + "\' AND dataSource = 1");
//			System.out.println(queryStr);
//			rs = stmt.executeQuery(queryStr);
//			int rowcount = 0;
//			if(rs.last()){
//				rowcount = rs.getRow();
//				System.out.println(rowcount);
//				rs.beforeFirst();
//			}
			int counter = 0;
			while(rs.next()){
				counter++;
				rID = rs.getInt("id");
				rStr = rs.getString("text");//subject to change
//				System.out.println("--DB-----:\t" + rStr);
				rLink = rs.getString("rLink");
				NEStr = rs.getString("food");
				taggedText = rs.getString("taggedText");				
				Review r = new Review(rID, rStr, taggedText, NEStr);
								
//				System.out.println("Tagged review:\t" + taggedText);
				r.setWebLink(rLink);
				r.setDataSource(rs.getInt("dataSource"));
				biz.getReviews().add(r);
			}
//			System.out.println("counter:\t" + counter);
//			System.out.println("fetch reviews from DB:\t" + (endTime - startTime));
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DBConnector.close(rs);
		}
		
//		System.out.println("business review number:" + biz.getReviews().size());
		return biz;
	}
	
	public Business insertFoods2DB(Business biz, Connection conn, PreparedStatement ps, PreparedStatement psInsertFoodReview){
		for(Food food : biz.getGoodFoods()){			
//			System.out.println("Inserting food:\t" + food.getFoodText());
			try {
				String foodID = UUID.randomUUID().toString();
				ps.setString(1, food.getFoodText());
				ps.setString(2, biz.getBusiness_id());
				ps.setInt(3, biz.getDataSource());
				ps.setString(4, biz.getBusiness_name());
				ps.setString(5, biz.getBusiness_address());
				ps.setFloat(6, Float.parseFloat(biz.getLatitude()));
				ps.setFloat(7, Float.parseFloat(biz.getLongitude()));
				ps.setString(8, biz.getBusiness_phone());
				ps.setString(9, biz.getWebsite());
				ps.setString(10, foodID);
				ps.execute();
				
				for(Review r : food.getReviewsGood()){
					psInsertFoodReview.setString(1, foodID);
					psInsertFoodReview.setInt(2, r.getrID());
					psInsertFoodReview.execute();
				}
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}			
//		System.out.println("business review number:" + biz.getReviews().size());
		return biz;
	}

	
	public Business writeInsertFoods2DB(Business biz, BufferedWriter bw1, BufferedWriter bw2) throws IOException{
		for(Food food : biz.getGoodFoods()){			
//			System.out.println("Inserting food:\t" + food.getFoodText());
			StringBuffer sb = new StringBuffer();
			List<String> insertValues = new ArrayList<String>();
			String foodID = UUID.randomUUID().toString();
			sb.append("(");
			insertValues.add("'" + food.getFoodText() + "'");
			insertValues.add("'" + biz.getBusiness_id() + "'");
			insertValues.add(String.valueOf(biz.getDataSource()));
			insertValues.add("'" + biz.getBusiness_name() + "'");
			insertValues.add("'" + biz.getBusiness_address() + "'");
			insertValues.add(String.valueOf(Float.parseFloat(biz.getLatitude())));
			insertValues.add(String.valueOf(Float.parseFloat(biz.getLongitude())));
			insertValues.add("'" + biz.getBusiness_phone() + "'");
			insertValues.add("'" + biz.getWebsite() + "'");
			insertValues.add("'" + foodID + "'");			
			String strInsertValues = StringUtils.join(insertValues, ", ");
			sb.append(strInsertValues);
			sb.append("),\n");
			bw1.write(sb.toString());
			
			
			for(Review r : food.getReviewsGood()){
				insertValues.clear();
				sb.setLength(0);
				sb.append("(");
				insertValues.add("'" + foodID + "'");
				insertValues.add(String.valueOf(r.getrID()));
				strInsertValues = StringUtils.join(insertValues, ", ");
				sb.append(strInsertValues);
				sb.append("),\n");
				bw2.write(sb.toString());
				
			}		
		}			
//		System.out.println("business review number:" + biz.getReviews().size());
		return biz;
	}
	
	public static int getNumBusiness() {
		return numBusiness;
	}

	public static void setNumBusiness(int numBusiness) {
		FoodDBProcessor.numBusiness = numBusiness;
	}


	public String getApiPrefixPlace() {
		return apiPrefixPlace;
	}

	public void setApiPrefixPlace(String apiPrefixPlace) {
		this.apiPrefixPlace = apiPrefixPlace;
	}

	public JsonParser getJsonParser() {
		return jsonParser;
	}

	public void setJsonParser(JsonParser jsonParser) {
		this.jsonParser = jsonParser;
	}


	public GoodFoodFinder getFinder() {
		return finder;
	}

	public void setFinder(GoodFoodFinder finder) {
		this.finder = finder;
	}

	
	
	public void clearHistoryData(){
		
	}
	
	public Configuration getConfig() {
		return config;
	}

	public void setConfig(Configuration config) {
		this.config = config;
	}

	

	public final String getApiPrefixReview() {
		return apiPrefixReview;
	}

	public final void setApiPrefixReview(String apiPrefixReview) {
		this.apiPrefixReview = apiPrefixReview;
	}

	public final String getApiSurfixReview() {
		return apiSurfixReview;
	}

	public final void setApiSurfixReview(String apiSurfixReview) {
		this.apiSurfixReview = apiSurfixReview;
	}
	
	public void writeSQL2File() throws IOException{
		List<Business> bizs = this.fetchPlacesFromDB("goodfood_biz");
		System.out.println("creating sql files");
		
		String pathFood = "D:\\projects\\FoodSearch\\data\\food\\goodfood_food_FourSquare.sql";
		BufferedWriter bwFood = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(pathFood))));
		String pathFood2Review = "D:\\projects\\FoodSearch\\data\\food\\gf_Food2Review_FourSquare.sql";
		BufferedWriter bwFood2Review = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(pathFood2Review))));
		
		bwFood.write("INSERT INTO goodfood_food_FourSquare (foodText,bizSrcID,dataSource,bizName,address,latitude,longitude,phoneNum,bizWebsite,id) VALUES \n");
		bwFood2Review.write("INSERT INTO gf_Food2Review_FourSquare (FoodID, ReviewID) VALUES \n");
//		System.out.println("stopped");
//		System.exit(0);
		int counter = 0;
		
		for(Business biz : bizs){
			if(counter%1000 == 0){
				System.out.println("Inserted \t" + counter + "\t business");
			}
			counter++;
			this.fetchReviewsFromDB(biz, this.dbconn, this.psSelectReviews);
			biz.extractInfoFromReviews();

//			this.insertFoods2DB(biz, this.dbconn, this.psInsertFoods, this.psInsertFood2Review);
			this.writeInsertFoods2DB(biz, bwFood, bwFood2Review);
//			for(Food food : biz.getGoodFoods()){
//				System.out.println(food.getFoodText());
//				System.out.println(food.getReviewsGood().size());	
//				for(Review r : food.getReviewsGood()){
//					System.out.println(r.getReviewStr());
//				}
//			}
		}
		
		bwFood.close();
		bwFood2Review.close();
	}

	public static void main(String[] args) throws IOException{
		System.out.println("exporting to sql!");
		FoodDBProcessor myProcessor = new FoodDBProcessor();
		myProcessor.writeSQL2File();
		
	}

}
