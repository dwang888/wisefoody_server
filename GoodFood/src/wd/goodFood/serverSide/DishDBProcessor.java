//package wd.goodFood.serverSide;
////just for getting data from DB; will be deperated after migratin to search engine
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.InputStreamReader;
//import java.io.Reader;
//import java.math.BigDecimal;
//import java.net.HttpURLConnection;
//import java.net.MalformedURLException;
//import java.net.URL;
//import java.net.URLConnection;
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.sql.Statement;
//import java.sql.Timestamp;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Calendar;
//import java.util.Date;
//import java.util.List;
//import java.util.Map.Entry;
//import java.util.Set;
//
//import org.apache.commons.lang.StringEscapeUtils;
//
//import wd.goodFood.entity.Business;
//import wd.goodFood.entity.Food;
//import wd.goodFood.entity.Review;
//import wd.goodFood.nlp.GoodFoodFinder;
//import wd.goodFood.utils.Configuration;
//import wd.goodFood.utils.DBConnector;
//
//import com.google.gson.Gson;
//import com.google.gson.JsonArray;
//import com.google.gson.JsonElement;
//import com.google.gson.JsonObject;
//import com.google.gson.JsonParser;
//import com.google.gson.stream.JsonReader;
//
//public class DishDBProcessor extends DataSourceProcessor{
//	private Configuration config;
//	private static int numBusiness;//how many business results should be returned per request.
//	private String apiPrefixPlace;	
//	//for fetcheding detail for each place, but only 3 reviews are returned for each place
//	private String apiPrefixReview;	
//	private String apiSurfixReview;	
//	private JsonParser jsonParser;//should be thread safe
//	private GoodFoodFinder finder;
//	
////	String INSERT_biz = "INSERT INTO goodfoodDB.goodfood_food_FourSquare "
////			+ "(bizName, address, bizSrcID, latitude, longitude, phoneNum, merchantMsg, offer, numReviews, profileLink, bizWebsite, dataSource, updateTime, category) VALUES"
////			+ "(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
//	
////	String INSERT_reviews = "INSERT INTO goodfoodDB.goodfood_reviews_FourSquare "
////			+ "(bizID, bizSrcID, text, rLink, taggedText, food, dataSource, insertTime, updateTime) VALUES"
////			+ "(?,?,?,?,?,?,?,?,?)";
//	
////	String SELECT_biz = "SELECT * FROM goodfoodDB.goodfood_biz_FourSquare WHERE bizSrcID = ?";
//	String SELECT_food = "SELECT * FROM goodfoodDB.goodfood_food_FourSquare WHERE bizSrcID = ?";
//		
////	String SELECT_reviews = "SELECT text,rLink,taggedText,food,dataSource,insertTime FROM goodfoodDB.goodfood_reviews_FourSquare WHERE bizSrcID = ? AND dataSource = ?";
////	String DELETE_reviews = "DELETE FROM goodfoodDB.goodfood_reviews_FourSquare WHERE bizSrcID = ? AND dataSource = ?";
//	
//	
////	public  DishDBProcessor(String configFile, GoodFoodFinder finder) throws Exception{
////		config = new Configuration(configFile);
////		this.setFinder(finder);
////		this.numBusiness = Integer.parseInt(config.getValue("numBusiness_FourSquare"));
////		this.apiPrefixPlace = config.getValue("apiPrefixPlace_FourSquare");
////		this.apiPrefixReview = config.getValue("apiPrefixReview_FourSquare");
////		this.apiSurfixReview = config.getValue("apiSurfixReview_FourSquare");
////		this.setJsonParser(new JsonParser());
////	}
//
//	public DishDBProcessor(){
//		this.setJsonParser(new JsonParser());
////		System.out.println("initilize BizDBProcessor");
//	}
//	
//	
//	
//	public List<Dish> fetchDishesFromDB(String dishDB, String dishID){
//		//use json here; an xml based version may be needed
////		System.out.println(lat);
//		String select_biz = "SELECT * FROM goodfoodDB." + dishDB + " WHERE bizSrcID = " + "'" + dishID + "'";
////		String select_biz = "SELECT * FROM goodfoodDB.goodfood_biz" + " WHERE bizSrcID = 758653";
//		
//		System.out.println("SQL query:\t" + select_biz);
//		long startTime = System.currentTimeMillis();
//		Connection dbconn = null;
//		PreparedStatement psSelectBiz = null;
//		
//		List<Food> dishes = new ArrayList<Food>();
//		Food  dish = new Food();
////		System.out.println("fetching PLACEs from:\t");
////		System.out.println(apiStr);		
//		try {
////			dbconn = GoodFoodServlet.DS.getConnection();
//			dbconn = GoodFoodRestaurantServlet.DS.getConnection();
//			Statement stmt = null;
//			
//			stmt = dbconn.createStatement();
//			ResultSet rs = stmt.executeQuery(select_biz);
//			while(rs.next()){
//				String id = rs.getString("id");
//				String foodText = rs.getString("foodText");
//				String bizSrcID = rs.getString("bizSrcID");
//				int dataSource = rs.getInt("dataSource");
//				String bizName = rs.getString("bizName");
//				String address = rs.getString("address");
//				String latitude = rs.getString("latitude");
//				String longitude = rs.getString("longitude");			
//				String phoneNum = rs.getString("phoneNum");
//				String bizWebsite = rs.getString("bizWebsite");
//				String updateTime = rs.getString("updateTime");
////				System.out.println(bizName + address);
//				
//				dish.setFoodText(foodText);
//				
//				dish.setBusiness_address(address);
//				dish.setLatitude(latitude);
//				dish.setLongitude(longitude);
//				dish.setBusiness_phone(phoneNum);
//				dish.setBusiness_id(bizID);
//				dish.setDataSource(dataSource);
//				dish.setBusiness_merchantMsg(merchantMsg);
//				dish.setBusiness_offer(offer);
//				dish.setNumReviews(numReviews);
//				dish.setWebsite(bizWebsite);		
//				
//				dishes.add(dish);
//				break;
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		} finally {
//			DBConnector.close(dbconn);
//			DBConnector.close(psSelectBiz);
//			
//		}		
//		
//		long endTime = System.currentTimeMillis();
////		System.out.println("time fetch PLACES from API:\t" + (endTime - startTime));
//		return dishes;
//	}
//	
//
//	public static int getNumBusiness() {
//		return numBusiness;
//	}
//
//	public static void setNumBusiness(int numBusiness) {
//		DishDBProcessor.numBusiness = numBusiness;
//	}
//
//
//	public String getApiPrefixPlace() {
//		return apiPrefixPlace;
//	}
//
//	public void setApiPrefixPlace(String apiPrefixPlace) {
//		this.apiPrefixPlace = apiPrefixPlace;
//	}
//
//	public JsonParser getJsonParser() {
//		return jsonParser;
//	}
//
//	public void setJsonParser(JsonParser jsonParser) {
//		this.jsonParser = jsonParser;
//	}
//
//
//	public GoodFoodFinder getFinder() {
//		return finder;
//	}
//
//	public void setFinder(GoodFoodFinder finder) {
//		this.finder = finder;
//	}
//
//	
//	
//	public void clearHistoryData(){
//		
//	}
//	
//	public Configuration getConfig() {
//		return config;
//	}
//
//	public void setConfig(Configuration config) {
//		this.config = config;
//	}
//
//	
//
//	public final String getApiPrefixReview() {
//		return apiPrefixReview;
//	}
//
//	public final void setApiPrefixReview(String apiPrefixReview) {
//		this.apiPrefixReview = apiPrefixReview;
//	}
//
//	public final String getApiSurfixReview() {
//		return apiSurfixReview;
//	}
//
//	public final void setApiSurfixReview(String apiSurfixReview) {
//		this.apiSurfixReview = apiSurfixReview;
//	}
//
//	public static void main(String[] args){
//
//	}
//
//}
