package wd.goodFood.serverSide;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.zip.CRC32;
import java.util.Set;

import wd.goodFood.entity.Business;
import wd.goodFood.entity.Food;
import wd.goodFood.entity.Review;
import wd.goodFood.nlp.GoodFoodFinder;
import wd.goodFood.utils.Configuration;
import wd.goodFood.utils.DBConnector;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

public class CityGridUpdater extends DataSourceProcessor{
	private Configuration config;
	private static int numBusiness;//how many business results should be returned per request.
//	private static String apiPrefix;
	private String apiPrefixPlace;	
	//for fetcheding detail for each place, but only 3 reviews are returned for each place
	private String apiPrefixPlaceDetail;	
	//for fetcheding reviews for each location; 50 reviews are returned; could get input of latitude longitude, or listing_id
	private String apiPrefixReviewDetail;	
	private JsonParser jsonParser;
	private GoodFoodFinder finder;
//	private Connection conn;
	private Connection dbconn = null;
	
	String INSERT_biz = "INSERT INTO goodfoodDB.goodfood_biz "
			+ "(bizName, address, bizSrcID, latitude, longitude, phoneNum, merchantMsg, offer, numReviews, profileLink, bizWebsite, dataSource, updateTime) VALUES"
			+ "(?,?,?,?,?,?,?,?,?,?,?,?,?)";
	
	String INSERT_reviews = "INSERT INTO goodfoodDB.goodfood_reviews "
			+ "(bizID, bizSrcID, text, rLink, taggedText, food, dataSource, insertTime, updateTime, checksum) VALUES"
			+ "(?,?,?,?,?,?,?,?,?,?)";
	
	String SELECT_biz = "SELECT * FROM goodfoodDB.goodfood_biz WHERE bizSrcID = ? AND dataSource = ?";
		
	String SELECT_reviews = "SELECT text,rLink,taggedText,food,dataSource,checksum FROM goodfoodDB.goodfood_reviews WHERE bizSrcID = ? AND dataSource = ?";
	String Lookup_reviews = "SELECT id FROM goodfoodDB.goodfood_reviews WHERE bizSrcID = ? AND dataSource = ?";
	String SELECT_reviewsChecksum = "SELECT checksum FROM goodfoodDB.goodfood_reviews WHERE bizSrcID = ? AND checksum = ?";
	
	
	public  CityGridUpdater(String configFile) throws Exception{
		config = new Configuration(configFile);
		this.numBusiness = Integer.parseInt(config.getValue("numBusiness"));
//		this.apiPrefix = config.getValue("apiPrefix");
		this.apiPrefixPlace = config.getValue("apiPrefixPlace") + this.numBusiness;
		this.apiPrefixPlaceDetail = config.getValue("apiPrefixPlaceDetail");
		this.apiPrefixReviewDetail = config.getValue("apiPrefixReviewDetail");
		this.setJsonParser(new JsonParser());
//		this.setBizs(new ArrayList<Business>());
		this.setFinder(new GoodFoodFinder(config.getValue("NETaggerPath")));
		
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
	}
	
	public  CityGridUpdater(String configFile, GoodFoodFinder finder) throws Exception{
		config = new Configuration(configFile);
		this.setFinder(finder);
		this.numBusiness = Integer.parseInt(config.getValue("numBusiness"));
//		this.apiPrefix = config.getValue("apiPrefix");
		this.apiPrefixPlace = config.getValue("apiPrefixPlace") + this.numBusiness;
		this.apiPrefixPlaceDetail = config.getValue("apiPrefixPlaceDetail");
		this.apiPrefixReviewDetail = config.getValue("apiPrefixReviewDetail");
		this.setJsonParser(new JsonParser());
//		this.setBizs(new ArrayList<Business>());		
	}
	
//	public CityGridInfoProcessor(String configFile, GoodFoodFinder finder, DBConnector dbconnector){
//		config = new Configuration(configFile);
//		this.setFinder(finder);
//		this.numBusiness = Integer.parseInt(config.getValue("numBusiness"));
//		this.apiPrefix = config.getValue("apiPrefix");
//		this.apiPrefixPlace = config.getValue("apiPrefixPlace") + this.numBusiness;
//		this.apiPrefixPlaceDetail = config.getValue("apiPrefixPlaceDetail");
//		this.apiPrefixReviewDetail = config.getValue("apiPrefixReviewDetail");
//		this.setJsonParser(new JsonParser());
//		this.setBizs(new ArrayList<Business>());	
//		this.dbconnector = dbconnector;
//	}
	
	/**
	 * add info from Json to Biz obj
	 * */
	public void addInfo2Biz(JsonObject jobj, Business biz){
		//use .toString() here to capture null; need to use .getAsString()?
		biz.setLatitude(jobj.get("latitude").toString().trim());
		biz.setLongitude(jobj.get("longitude").toString().trim());
		biz.setBusiness_id(jobj.get("id").toString().trim());
		biz.setBusiness_name(jobj.get("name").toString().trim().replace("\"", ""));
//		biz.setBusiness_address(jobj.get("address").toString().trim().replace("\"", ""));
		biz.setBusiness_address(cleanAddr(jobj.get("address").toString()));		
		biz.setBusiness_phone(jobj.get("phone_number").toString().trim().replace("\"", ""));
		biz.setBusiness_merchantMsg(jobj.get("profile").toString().trim());
		biz.setBusiness_offer(jobj.get("offers").toString().trim());
		if(jobj.get("user_review_count") != null){
			biz.setNumReviews(jobj.get("user_review_count").getAsInt());
		}		
//		System.out.println(jobj.get("rating").toString());
		biz.setRating(jobj.get("rating").toString().trim());
		biz.setWebsite(jobj.get("website").toString().trim().replace("\"", ""));
		biz.setLink(jobj.get("profile").toString().trim().replace("\"", ""));
		biz.setDataSource(1);
	}
	
	public synchronized List<Business> updatePlaces(String lat, String lon){
//	public List<Business> fetchPlaces(String lat, String lon){
		//use java URL instead of HtmlUnit to save memory
		//use json here; an xml based version may be needed
		long startTime = System.currentTimeMillis();
//		Connection dbconn = null;
		PreparedStatement psSelectBiz = null;
		PreparedStatement psInsertBiz = null;
		String apiStr = this.getApiPrefixPlace() + "&lat=" + lat + "&lon=" + lon;
		List<Business> bizs = new ArrayList<Business>();
		System.out.println("fetching CityGrid PLACEs from:\t");
		System.out.println(apiStr);		
		try {
//			dbconn = GoodFoodServlet.DS.getConnection();
			psSelectBiz = dbconn.prepareStatement(this.SELECT_biz);
			psInsertBiz = dbconn.prepareStatement(this.INSERT_biz);
			URL url = new URL(apiStr);
			HttpURLConnection urlconn = (HttpURLConnection) url.openConnection();
			InputStream is = urlconn.getInputStream();			
			JsonReader reader = new JsonReader(new InputStreamReader(is));
			JsonObject jobj= jsonParser.parse(reader).getAsJsonObject();
			DBConnector.close(is);
			JsonArray locations = jobj.getAsJsonObject("results").getAsJsonArray("locations");
//			System.out.println(locations.size());
			
			for(JsonElement location : locations){
				JsonObject loc = (JsonObject)location;
				Business biz = new Business();
				this.addInfo2Biz(loc, biz);
//				if(this.getConn() != null && !this.isBizInDB(biz)){
				if(!this.isBizInDB(biz, dbconn, psSelectBiz)){
					this.addBiz2DB(biz, dbconn, psInsertBiz);
				}				
//				System.out.println(loc.get("latitude").getAsString());
				bizs.add(biz);
			}			
			
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
//			DBConnector.close(dbconn);
			DBConnector.close(psSelectBiz);
			DBConnector.close(psInsertBiz);
			
		}
		
		
		long endTime = System.currentTimeMillis();
//		System.out.println("PLACES fetching cost:\t" + (endTime - startTime));
//		this.setBizs(bizs);
		return bizs;
	}	
	
	/**
	 * collect all the reviews for each business
	 * */
//	public synchronized List<Business> fetchReviews(List<Business> bizs){
	public List<Business> fetchReviews(List<Business> bizs){
//		Connection dbconn = null;
		PreparedStatement psSelectReview = null;
		PreparedStatement psLookupReview = null;
		PreparedStatement psInsertReview = null;
		PreparedStatement psSelectReviewChecksum = null;
		long start;
		long end;
		try {
//			dbconn = GoodFoodServlet.DS.getConnection();
			psSelectReview = dbconn.prepareStatement(this.SELECT_reviews);
			psSelectReviewChecksum = dbconn.prepareStatement(this.SELECT_reviewsChecksum);
//			psLookupReview = dbconn.prepareStatement(this.Lookup_reviews);
			psInsertReview = dbconn.prepareStatement(this.INSERT_reviews);
			for(Business biz : bizs){
				start = System.currentTimeMillis();
				biz = this.fetchReviewsFromAPI(biz);
//				boolean flag = this.isInReviewTable(biz, dbconn, psLookupReview);
//				boolean flag = this.isInReviewTable(biz, dbconn, psSelectReview);
				end = System.currentTimeMillis();
//				System.out.println("------------------------------------------------>");
//				System.out.println("time for judeg based on review table:\t" + (end - start));
				if(biz.getReviews().size() != 0){
					this.addReviews2DB(biz, dbconn, psInsertReview, psSelectReviewChecksum);
				}
	
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
//			DBConnector.close(dbconn);
			DBConnector.close(psSelectReview);
			DBConnector.close(psInsertReview);
			DBConnector.close(psLookupReview);
		}		
		
		return bizs;
	}
	
	/**
	 * store business reviews to DB, as cache
	 * */
	public void addReviews2DB(Business biz, Connection conn, PreparedStatement ps, PreparedStatement psSelectReviewChecksum){
//		System.out.println(biz.getBusiness_id());
//		Connection conn = null;
//		PreparedStatement ps =null;	
		int numIn = 0;
		int numNotIn = 0;
		
		try {
//			conn = GoodFoodServlet.ds.getConnection();
//			ps = conn.prepareStatement(this.INSERT_reviews);
			List<Review> reviews = biz.getReviews();
			if(reviews == null || reviews.size() == 0){
				return;
			}
//			System.out.println(reviews.size());
			for(Review r : reviews){
				boolean inDB = isReviewInDB(biz, r.getReviewStr(), psSelectReviewChecksum);
				if(inDB == false){
					ps.setInt(1, 0);//how to get biz id?
					ps.setString(2, biz.getBusiness_id());
					ps.setString(3, r.getReviewStr());
					ps.setString(4, r.getWebLink());
					ps.setString(5, r.getTaggedStr());//later, will be not null
					ps.setString(6, r.getNEStr());//later, will be not null
					ps.setInt(7, r.getDataSource());
					ps.setTimestamp(8, new Timestamp(System.currentTimeMillis()));
					ps.setTimestamp(9, new Timestamp(System.currentTimeMillis()));
					CRC32 crc = new CRC32();
					crc.update(r.getReviewStr().getBytes());
//					System.out.println("checksum value!!!" + (int) crc.getValue() + "|--|" +crc.getValue());
					ps.setLong(10, crc.getValue());		
					ps.execute();
//					System.out.println("++++++++++++++++++++++++++");
//					System.out.println("inserting a review++++++++");
//					System.out.println(r.getReviewStr());
					numNotIn++;
				}else{
					numIn++;
				}				
			}
			
			System.out.println("In and Not In:\t" + numIn + "<--->" + numNotIn);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally{
//			close(ps);
//			close(conn);
		}		
	}
	
	public boolean isReviewInDB(Business biz, String rText, PreparedStatement ps){
		Boolean inTable = false;
		ResultSet rs = null;
//		long checkSum = crc.getValue();
		try {			
//			conn = GoodFoodServlet.ds.getConnection();
//			ps = conn.prepareStatement(this.SELECT_reviews);
			ps.setString(1, biz.getBusiness_id());
			CRC32 crc = new CRC32();
			crc.update(rText.getBytes());
			ps.setLong(2, crc.getValue());
//			ps.setLong(2, 4149384845);
			rs = ps.executeQuery();
			inTable = rs.isAfterLast() == rs.isBeforeFirst()? false : true;
//			System.out.println(biz.getBusiness_id() + "---||---" + crc.getValue() + "||" + rText);
			if(inTable == true){
//				System.out.println("|||||||| in DB:");
//				System.out.println(biz.getBusiness_id() + "||" + crc.getValue() + "||" + rText);
			}else{
//				System.out.println("XXXXXXXX notin DB:");
//				System.out.println(biz.getBusiness_id() + "|XX|" + crc.getValue() + "|XX|" + rText);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DBConnector.close(rs);
//			close(ps);
//			close(conn);
		}
		return inTable;
	}
	
	public Business fetchReviewsFromAPI(Business biz){
		long startTime = System.currentTimeMillis();
		String id = biz.getBusiness_id();
		String apiStr = this.getApiPrefixReviewDetail() + id;
		long time = 0;
		int length = 0;
//		System.out.println("fetching reviews from:\n" + apiStr);
		try {
			URL url = new URL(apiStr);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			InputStream is = conn.getInputStream();			
			JsonReader reader = new JsonReader(new InputStreamReader(is));
			JsonObject jobj= jsonParser.parse(reader).getAsJsonObject();
			is.close();
			JsonObject results = jobj.getAsJsonObject("results");
			JsonArray bizReviews = results.getAsJsonArray("reviews");
//			System.out.println(bizReviews.toString());
			for(JsonElement rJelem : bizReviews){
				JsonObject robj = (JsonObject) rJelem;
				String rStr = robj.get("review_text").toString().trim();
				String rLink = robj.get("review_url").toString().trim().replace("\"", "");					
				long start = System.currentTimeMillis();
				Review r = this.getFinder().process(rStr);//call NLP tools
//				System.out.println("++API+++++:\t" + rStr);
				long end = System.currentTimeMillis();
//				System.out.println("Generate a review:\t" + ( endTime - startTime));
				r.setWebLink(rLink);
				r.setDataSource(1);//hardcode
				biz.getReviews().add(r);
				time += (end-start);
				length += rStr.length();
//				System.out.println(biz.getReviews().size());
			}
//			System.out.println(biz.getReviews().size());
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		long endTime = System.currentTimeMillis();
//		System.out.println("---------------->");
//		System.out.println("length of review:\t" + length);
//		System.out.println("NER from API:\t" + time);
//		System.out.println("fetch reviews from API:\t" + (endTime - startTime));
		return biz;
	}
	
	
	
	public static int getNumBusiness() {
		return numBusiness;
	}

	public static void setNumBusiness(int numBusiness) {
		CityGridUpdater.numBusiness = numBusiness;
	}

//	public static String getApiPrefix() {
//		return apiPrefix;
//	}

//	public static void setApiPrefix(String apiPrefix) {
//		CityGridInfoProcessor.apiPrefix = apiPrefix;
//	}

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


	public String getApiPrefixPlaceDetail() {
		return apiPrefixPlaceDetail;
	}

	public void setApiPrefixPlaceDetail(String apiPrefixPlaceDetail) {
		this.apiPrefixPlaceDetail = apiPrefixPlaceDetail;
	}


	public String getApiPrefixReviewDetail() {
		return apiPrefixReviewDetail;
	}

	public void setApiPrefixReviewDetail(String apiPrefixReviewDetail) {
		this.apiPrefixReviewDetail = apiPrefixReviewDetail;
	}

	public GoodFoodFinder getFinder() {
		return finder;
	}

	public void setFinder(GoodFoodFinder finder) {
		this.finder = finder;
	}

	/**
	 * remove the strang symbols and normalize address string
	 * */
	public String cleanAddr(String origAddr){
		if(origAddr == null || origAddr.length() == 0){
			return "";
		}
		
		JsonObject jObj= jsonParser.parse(origAddr).getAsJsonObject();
		StringBuilder sb = new StringBuilder();
		Set<Entry<String, JsonElement>> entries = jObj.entrySet();
		int i = 0;
		for(Entry<String, JsonElement> entry : entries){
			if(i == 0){
				sb.append(entry.getValue().getAsString().toString());
			}else{
				sb.append(", " + entry.getValue().getAsString().toString());
			}			
			i++;
		}
//		System.out.println(sb.toString());
		return sb.toString();
	}
	
	@Deprecated
	public String cleanAddr2(String origAddr){
		
		if(origAddr == null || origAddr.length() == 0){
			return "";
		}
		StringBuilder sb = new StringBuilder();		
		origAddr = origAddr.trim().replace("\"", "");
		origAddr = origAddr.trim().replace("{", "");
		origAddr = origAddr.trim().replace("}", "");
		String[] strSplits = origAddr.split(",");
		for(int i = 0; i < strSplits.length; i++){
			String[] splitTmp = strSplits[i].split(":");
			String realAddr;
			if(splitTmp.length == 0){
				realAddr = splitTmp[0];
			}else{
				realAddr = splitTmp[1];
			}
			if(i == 0){
				sb.append(realAddr);
			}else{
				sb.append(", " + realAddr);
			}			
		}
		return sb.toString();
	}
	
	public void clearHistoryData(){
		
	}
	
	public Configuration getConfig() {
		return config;
	}

	public void setConfig(Configuration config) {
		this.config = config;
	}


	public static void main(String[] args){
		if(args.length < 1){
			System.out.println("no property file input!!!");
			System.exit(0);
		}
		CityGridUpdater processor = null;
		try {
			processor = new CityGridUpdater(args[0]);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		processor.clearHistoryData();
		String lat = "39.6141019893";
		String lon = "-121.395812287";
		List<Business> bizs = processor.updatePlaces(lat, lon);		
		bizs = processor.fetchReviews(bizs);
	}

}
