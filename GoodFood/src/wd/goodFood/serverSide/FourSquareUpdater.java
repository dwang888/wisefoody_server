package wd.goodFood.serverSide;
//just for caching purpose
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.zip.CRC32;
import java.util.Set;

import org.apache.commons.lang.StringEscapeUtils;

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

public class FourSquareUpdater extends DataSourceProcessor{
	private Configuration config;
	private static int numBusiness;//how many business results should be returned per request.
	private String apiPrefixPlace;	
	//for fetcheding detail for each place, but only 3 reviews are returned for each place
	private String apiPrefixReview;	
	private String apiSurfixReview;	
	private JsonParser jsonParser;//should be thread safe
	private GoodFoodFinder finder;
	private String dbTableName = "goodfood_biz_FourSquare";
	private Connection dbconn = null;
	
	String INSERT_biz = "INSERT INTO goodfoodDB.goodfood_biz_FourSquare "
			+ "(bizName, address, bizSrcID, latitude, longitude, phoneNum, merchantMsg, offer, numReviews, profileLink, bizWebsite, dataSource, updateTime, category) VALUES"
			+ "(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	
	String INSERT_reviews = "INSERT INTO goodfoodDB.goodfood_reviews_FourSquare "
			+ "(bizID, bizSrcID, text, rLink, taggedText, food, dataSource, insertTime, updateTime, checksum) VALUES"
			+ "(?,?,?,?,?,?,?,?,?,?)";
	
	String SELECT_biz = "SELECT * FROM goodfoodDB.goodfood_biz_FourSquare WHERE bizSrcID = ? AND dataSource = ?";
		
	String SELECT_reviews = "SELECT text,rLink,taggedText,food,dataSource,insertTime,checksum FROM goodfoodDB.goodfood_reviews_FourSquare WHERE bizSrcID = ? AND dataSource = ?";
	String SELECT_reviewsChecksum = "SELECT checksum FROM goodfoodDB.goodfood_reviews_FourSquare WHERE bizSrcID = ? AND checksum = ?";
	
	String DELETE_reviews = "DELETE FROM goodfoodDB.goodfood_reviews_FourSquare WHERE bizSrcID = ? AND dataSource = ?";
	
	
	public  FourSquareUpdater(String configFile, GoodFoodFinder finder) throws Exception{
		config = new Configuration(configFile);
		this.setFinder(finder);
		this.numBusiness = Integer.parseInt(config.getValue("numBusiness_FourSquare"));
		this.apiPrefixPlace = config.getValue("apiPrefixPlace_FourSquare");
		this.apiPrefixReview = config.getValue("apiPrefixReview_FourSquare");
		this.apiSurfixReview = config.getValue("apiSurfixReview_FourSquare");
		this.setJsonParser(new JsonParser());
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

	
	public  FourSquareUpdater(String configFile) throws Exception{
		config = new Configuration(configFile);
		this.numBusiness = Integer.parseInt(config.getValue("numBusiness_FourSquare"));
		this.apiPrefixPlace = config.getValue("apiPrefixPlace_FourSquare");
		this.apiPrefixReview = config.getValue("apiPrefixReview_FourSquare");
		this.apiSurfixReview = config.getValue("apiSurfixReview_FourSquare");
		this.setJsonParser(new JsonParser());
//		this.setFinder(new GoodFoodFinder(config.getValue("sentSplitterPath"), config.getValue("tokenizerPath"), config.getValue("NETaggerPath")));
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
	
	/**
	 * particularly for FourSquare
	 * */
	public String extractAddress(JsonObject location){
		JsonElement eTmp = null;
		StringBuilder sb = new StringBuilder();
		eTmp = location.get("address");
		if(eTmp != null){
			sb.append(eTmp.getAsString() + ", ");
		}
		eTmp = location.get("city");
		if(eTmp != null){
			sb.append(eTmp.getAsString() + ", ");
		}
		eTmp = location.get("state");
		if(eTmp != null){
			sb.append(eTmp.getAsString());
		}
		return sb.toString();
	}
	
	/**
	 * add info from FourSquare Json response to Biz obj
	 * */
	public Business addInfo2Biz(JsonObject jobj, Business biz){
		JsonElement eTmp = null;
		JsonObject oTmp = null;
			
		biz.setBusiness_id(jobj.get("id").getAsString().trim());//pure biz id
//		biz.setBusiness_id(this.dbTableName + "__" + jobj.get("id").getAsString().trim());//use db table name + biz id; globally unique
		biz.setBusiness_name(jobj.get("name").getAsString().trim());
		
		eTmp = jobj.getAsJsonObject("contact").get("phone");
		if(eTmp != null){
			biz.setBusiness_phone(eTmp.getAsString());
		}

		oTmp = jobj.getAsJsonObject("location");
		eTmp = oTmp.get("address");
		biz.setLatitude(oTmp.get("lat").getAsString());
		biz.setLongitude(oTmp.get("lng").getAsString());		
		biz.setBusiness_address(this.extractAddress(oTmp));
		
		oTmp = jobj.getAsJsonObject("stats");
		eTmp = oTmp.get("tipCount");
		biz.setNumReviews(Integer.parseInt(eTmp.getAsString()));
//		System.out.println(eTmp.getAsString());
		
		oTmp = jobj.getAsJsonObject("description");
		if(oTmp != null){
			biz.setBusiness_merchantMsg(oTmp.getAsString());
		}
		eTmp = jobj.get("url");
		if(oTmp != null){
			biz.setWebsite(jobj.get("url").getAsString());
			biz.setLink(jobj.get("url").getAsString());
		}
		
		//no such rating in new version foursquare api since 201309
//		if(jobj.get("rating") != null){
//			biz.setRating(jobj.get("rating").getAsString());
//		}
				
//		oTmp = jobj.getAsJsonObject("categories");
		JsonArray arrayTmp = jobj.getAsJsonArray("categories");//only consider the first category
		if(arrayTmp != null && arrayTmp.get(0) != null){
			String catID = arrayTmp.get(0).getAsJsonObject().get("id").getAsString();
			biz.setCategory(catID);
		}
		
		biz.setDataSource(2);//TODO:hardcode?	
		return biz;
	}
	
	public List<Business> fetchPlaces(String lat, String lon){
		//use json here; an xml based version may be needed
//		System.out.println(lat);
		long startTime = System.currentTimeMillis();
		Connection dbconn = null;
		PreparedStatement psSelectBiz = null;
		PreparedStatement psInsertBiz = null;
		String apiStr = this.getApiPrefixPlace() + "&ll=" + lat + "," + lon + "&categoryId=4d4b7105d754a06374d81259";//TODO: sharp the category tree
		String today = new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime());
		apiStr += "&v=" + today;
		
		List<Business> bizs = new ArrayList<Business>();
//		System.out.println("fetching PLACEs from:\t");
//		System.out.println(apiStr);		
		try {
			dbconn = GoodFoodServlet.DS.getConnection();
			psSelectBiz = dbconn.prepareStatement(this.SELECT_biz);
//			psSelectBiz = null;
			psInsertBiz = dbconn.prepareStatement(this.INSERT_biz);
//			psInsertBiz = null;
			URL url = new URL(apiStr);
			HttpURLConnection urlconn = (HttpURLConnection) url.openConnection();
			InputStream is = urlconn.getInputStream();	
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			StringBuilder sb = new StringBuilder();
			String line;
			while((line = br.readLine()) != null){
//				System.out.println(line);
				sb.append(line);
			}
			
//			JsonReader reader = new JsonReader(new InputStreamReader(is));
//			JsonObject jobj= jsonParser.parse(reader).getAsJsonObject();
			String objStr = sb.toString();
//			objStr = objStr.replaceAll("(?<![\"}\\]\\de]),\"", "\",\"");
			
//			System.out.println(sb.toString());
//			System.out.println(objStr);
			JsonObject jobj= (JsonObject)jsonParser.parse(objStr);
			
//			Gson gson = new Gson();
//			Phone fooFromJson = gson.f.fromJson(jsonString, Phone.class);
			DBConnector.close(is);
			br.close();
//			JsonArray groups = jobj.getAsJsonObject("response").getAsJsonArray("groups");
//			JsonArray locations = groups.get(0).getAsJsonObject().getAsJsonArray("items");
			JsonArray locations = jobj.getAsJsonObject("response").getAsJsonArray("venues");
			System.out.println("number of locations:\t" + locations.size());
			
			for(JsonElement location : locations){
				JsonObject loc = (JsonObject)location;
				Business biz = new Business();
				this.addInfo2Biz(loc, biz);
				if(!this.isBizInDB(biz, dbconn, psSelectBiz)){
					this.addBiz2DB(biz, dbconn, psInsertBiz);
//					System.out.println(biz.getBusiness_name());
				}
				bizs.add(biz);
			}			
			
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
//			DBConnector.close(dbconn);
			DBConnector.close(psSelectBiz);
			DBConnector.close(psInsertBiz);
			
		}		
		
		long endTime = System.currentTimeMillis();
//		System.out.println("time fetch PLACES from API:\t" + (endTime - startTime));
		return bizs;
	}
	
	/**
	 * store business info to DB, as cache
	 * */
	public void addBiz2DB(Business biz, Connection conn, PreparedStatement ps){
		try {
			ps.setString(1, biz.getBusiness_name());
			ps.setString(2, biz.getBusiness_address());
			ps.setString(3, biz.getBusiness_id());
			ps.setBigDecimal(4, new BigDecimal(biz.getLatitude()));
			ps.setBigDecimal(5, new BigDecimal(biz.getLongitude()));
			ps.setString(6, biz.getBusiness_phone());
			ps.setString(7, biz.getBusiness_merchantMsg());
			ps.setString(8, biz.getBusiness_offer());
			ps.setInt(9, 0);//TODO:subject to change to real number; number of reviews
			ps.setString(10, biz.getLink());
			ps.setString(11, biz.getWebsite());
			ps.setInt(12, biz.getDataSource());//data source; here it should be 2 for FourSquare
			ps.setTimestamp(13, new Timestamp(System.currentTimeMillis()));
			ps.setString(14, biz.getCategory());
			ps .executeUpdate();
		} catch (SQLException e1) {
			e1.printStackTrace();
		} finally {
//			close(ps);
//			close(conn);
		}

	}
	
	//and check if 60 days ago, only for FourSquare
	@Override
	public boolean isInReviewTable(Business biz, Connection conn, PreparedStatement ps){
		Boolean inTable = false;
		ResultSet rs = null;		
//		PreparedStatement ps = null;
//		Connection conn = null;
		try {			
//			conn = GoodFoodServlet.ds.getConnection();
//			ps = conn.prepareStatement(this.SELECT_reviews);
			ps.setString(1, biz.getBusiness_id());
			ps.setInt(2, biz.getDataSource());
			rs = ps.executeQuery();
			inTable = rs.isAfterLast() == rs.isBeforeFirst()? false : true;
			if(true == inTable){//check if 30 days ago				
				Calendar calendar = Calendar.getInstance();
				calendar.add(Calendar.MONTH, -2);
				Timestamp nowNdays = new Timestamp(calendar.getTimeInMillis());	
				rs.next();
				Timestamp insertTime = rs.getTimestamp("insertTime");
//				System.out.println(insertTime);
//				if(!insertTime.after(nowNdays)){//TODO: hardcoded column name
//					//reviews 30 days ago
//					inTable = false;
//					PreparedStatement psDeleteReview = conn.prepareStatement(this.DELETE_reviews);
//					this.deleteReviewsFromDB(biz, conn, psDeleteReview);//TODO: remove from here?
//				}
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
	
	/**
	 * collect all the reviews for each business
	 * */
//	public synchronized List<Business> fetchReviews(List<Business> bizs){
	public List<Business> fetchReviews(List<Business> bizs){
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
				Thread.sleep(1000);
				start = System.currentTimeMillis();
				biz = this.fetchReviewsFromAPI(biz);
				boolean flagInDB = this.isInReviewTable(biz, dbconn, psSelectReview);				
				biz = this.fetchReviewsFromAPI(biz);
				if(biz.getReviews().size() != 0){
						this.addReviews2DB(biz, dbconn, psInsertReview, psSelectReviewChecksum);//add to cache
						end = System.currentTimeMillis();
						
				}
				end = System.currentTimeMillis();
//				System.out.println("fetch reviews from APIIII:\t" + (end - start));
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  finally {
//			DBConnector.close(dbconn);
			DBConnector.close(psSelectReview);
			DBConnector.close(psInsertReview);
			DBConnector.close(psLookupReview);
		}
		return bizs;
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
//			System.out.println("In and Not In:\t" + numIn + "<--->" + numNotIn);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally{
//			close(ps);
//			close(conn);
		}		
	}
	
	//fetch reviews from API and add them to biz class
	public Business fetchReviewsFromAPI(Business biz){
		long startTime = System.currentTimeMillis();
		String id = biz.getBusiness_id();
		String apiStr = this.getApiPrefixReview() + id + this.getApiSurfixReview();
//		String apiStr = this.getApiPrefixReview() + id + this.getApiSurfixReview() + "&v=20130518";
		String today = new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime());
		apiStr += "&v=" + today;
//		System.out.println(apiStr);
		long time = 0;
		int length = 0;
		try {
			URL url = new URL(apiStr);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			InputStream is = conn.getInputStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			StringBuilder sb = new StringBuilder();
			String line;
			while((line = br.readLine()) != null){
				sb.append(line);
			}
			String jsonStr = sb.toString();
			jsonStr = jsonStr.replaceAll("(?<![\"}\\]\\d,(true)(false)]),\"", "\",\"");
			jsonStr = jsonStr.replaceAll("\",\",\"", "\",\"");
//			System.out.println(jsonStr);
			JsonObject jobj= (JsonObject)jsonParser.parse(jsonStr);
//			JsonReader reader = new JsonReader(new InputStreamReader(is));
//			JsonObject jobj= jsonParser.parse(reader).getAsJsonObject();
			is.close();
			br.close();
			JsonObject tips = jobj.getAsJsonObject("response").getAsJsonObject("tips");
			if(tips != null){
				JsonArray bizReviews = tips.getAsJsonArray("items");
//				System.out.println(bizReviews.size() + "\t reviews");
				for(JsonElement jElem : bizReviews){
					JsonObject robj = (JsonObject) jElem;
//					String rStr = robj.get("text").toString().trim() + " @ via FourSquare";
					String rStr = robj.get("text").toString().trim();
					String rLink = robj.get("canonicalUrl").getAsString().trim();
					
					long start = System.currentTimeMillis();
//					System.out.println("-----\n" + rStr);
					Review r = this.getFinder().process(rStr);//call NLP tools
					long end = System.currentTimeMillis();
					r.setWebLink(rLink);
					r.setDataSource(2);//TODO: change hardcode 2?
					biz.getReviews().add(r);
					time += (end-start);
					length += rStr.length();
//					System.out.println(biz.getReviews().size());
				}
//				System.out.println(biz.getReviews().size());
			}
//			
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return biz;
		} catch (IOException e) {
			e.printStackTrace();
			return biz;
		}
		long endTime = System.currentTimeMillis();
//		System.out.println("---------------->");
//		System.out.println("length of review:\t" + length);
//		System.out.println("NER from API:\t" + time);
//		System.out.println("fetch reviews from API:\t" + (endTime - startTime));
		return biz;
	}	
	
	/**
	 * to fetch restaurants info and update DB
	 * */
	public List<Business> updatePlaces(String lat, String lon){
		//use json here; an xml based version may be needed
//		System.out.println(lat);
		long startTime = System.currentTimeMillis();
//		Connection dbconn = null;
		PreparedStatement psSelectBiz = null;
		PreparedStatement psInsertBiz = null;
		String apiStr = this.getApiPrefixPlace() + "&ll=" + lat + "," + lon + "&categoryId=4d4b7105d754a06374d81259";//TODO: sharp the category tree
		String today = new SimpleDateFormat("yyyyMMdd").format(Calendar.getInstance().getTime());
		apiStr += "&v=" + today;
		
		List<Business> bizs = new ArrayList<Business>();
//		System.out.println("fetching PLACEs from:\t");
//		System.out.println(apiStr);		
		try {
//			dbconn = GoodFoodUpdateServlet.DS.getConnection();

			psSelectBiz = dbconn.prepareStatement(this.SELECT_biz);
//			psSelectBiz = null;
			psInsertBiz = dbconn.prepareStatement(this.INSERT_biz);
//			psInsertBiz = null;
			URL url = new URL(apiStr);
			HttpURLConnection urlconn = (HttpURLConnection) url.openConnection();
			InputStream is = urlconn.getInputStream();	
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			StringBuilder sb = new StringBuilder();
			String line;
			while((line = br.readLine()) != null){
//				System.out.println(line);
				sb.append(line);
			}
			
//			JsonReader reader = new JsonReader(new InputStreamReader(is));
//			JsonObject jobj= jsonParser.parse(reader).getAsJsonObject();
			String objStr = sb.toString();
//			objStr = objStr.replaceAll("(?<![\"}\\]\\de]),\"", "\",\"");
			
//			System.out.println(sb.toString());
//			System.out.println(objStr);
			JsonObject jobj= (JsonObject)jsonParser.parse(objStr);
			
//			Gson gson = new Gson();
//			Phone fooFromJson = gson.f.fromJson(jsonString, Phone.class);
			DBConnector.close(is);
			br.close();
//			JsonArray groups = jobj.getAsJsonObject("response").getAsJsonArray("groups");
//			JsonArray locations = groups.get(0).getAsJsonObject().getAsJsonArray("items");
			JsonArray locations = jobj.getAsJsonObject("response").getAsJsonArray("venues");
//			System.out.format("locations around %s %s is %d \n", lat, lon, locations.size());
			
			for(JsonElement location : locations){
				JsonObject loc = (JsonObject)location;
				Business biz = new Business();
				this.addInfo2Biz(loc, biz);
				if(!this.isBizInDB(biz, dbconn, psSelectBiz)){
					this.addBiz2DB(biz, dbconn, psInsertBiz);
				}
				bizs.add(biz);
//				System.out.println("added\t" + " || " + biz.getBusiness_name());
			}			
			
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
//			DBConnector.close(dbconn);
			DBConnector.close(psSelectBiz);
			DBConnector.close(psInsertBiz);
			
		}		
		
		long endTime = System.currentTimeMillis();
//		System.out.println("time fetch PLACES from API:\t" + (endTime - startTime));
		return bizs;
	}
	
	public List<Business> addDBTableName(List<Business> bizs){
		for(Business biz : bizs){
			biz.setBusiness_id(this.dbTableName + "__" + biz.getBusiness_id());
		}
		return bizs;
	}

	public static int getNumBusiness() {
		return numBusiness;
	}

	public static void setNumBusiness(int numBusiness) {
		FourSquareUpdater.numBusiness = numBusiness;
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

	public static void main(String[] args){
		if(args.length < 1){
			System.out.println("no property file input!!!");
			System.exit(0);
		}
		FourSquareUpdater processor = null;
		try {
//			processor = new FourSquareInfoProcessor(args[0]);
			processor = new FourSquareUpdater(args[0]);
		} catch (Exception e) {
			e.printStackTrace();
		}
//		System.out.println(processor.apiSurfixReview);
//		processor.clearHistoryData();
		String lat = "33.9388100309";
		String lon = "-118.402768344";
		List<Business> bizs = processor.updatePlaces(lat, lon);		
		bizs = processor.fetchReviews(bizs);

	}

}
