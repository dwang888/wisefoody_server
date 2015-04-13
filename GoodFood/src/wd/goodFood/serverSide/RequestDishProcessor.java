package wd.goodFood.serverSide;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import wd.goodFood.entity.Business;
import wd.goodFood.entity.BusinessData;
import wd.goodFood.entity.Food;
import wd.goodFood.entity.FoodData;
import wd.goodFood.nlp.GoodFoodFinder;
import wd.goodFood.nlp.PostProcessor;
import wd.goodFood.serverSide.CityGridInfoProcessor;
import wd.goodFood.serverSide.InfoProcessor;
import wd.goodFood.utils.Configuration;
import wd.goodFood.utils.DBConnector;
import wd.goodFood.utils.EntityProcessor;

public class RequestDishProcessor {
	private Configuration config;
//	String testURL = "http://127.0.0.1:8080/dish/where?id=goodfood_food_FourSquare__8fcdff3e-852b-11e4-a36c-040124201a01&callback=?";
	Gson gson = new Gson();	

	class DishMeta{
		String dishID = null;
		String dishText = null;
		String bizSrcID = null;
		int dataSource = 1;
		String bizName = null;
		String address = null;
		String latitude = null;
		String longitude = null;
		String phoneNum = null;
		String bizWebsite = null;
		String updateTime = null;
	}	
	
	public RequestDishProcessor() throws Exception{
	}
	
	public RequestDishProcessor(String pathConfig) throws Exception{
		this.config = new Configuration(pathConfig);
//		this.finder = new GoodFoodFinder(config.getValue("sentSplitterPath"), 
//				config.getValue("tokenizerPath"), 
//				config.getValue("NETaggerPath"));
//		cityGridProcessor = new CityGridInfoProcessor(pathConfig, this.finder);
//		fourSquareInfoProcessor = new FourSquareInfoProcessor(pathConfig, this.finder);
//		bizDBProcessor = new BizDBProcessor();
	}
	
	public String callProcessors(HttpServletRequest req){
		String id = req.getParameter("id");
		if(id == null || id.equalsIgnoreCase("")){
			System.out.println("an invalid id");
		}
		String[] items = id.split("__");
		if(items == null || items.length != 2){
			System.out.println("invalid id partition");
		}
		String dbName = items[0];
		String dishID = items[1];
		
//		System.out.println("api query id:\t" + id);
//		System.out.println("db and food id:\t" + id);
		
        Boolean pretty = Boolean.parseBoolean(req.getParameter("pretty"));
//        String version = req.getParameter("version");
        

        String select_biz = "SELECT * FROM goodfoodDB." + dbName + " WHERE id = " + "'" + dishID + "'";
        Connection dbconn = null;
		PreparedStatement psSelectBiz = null;
		DishMeta  dishMeta = new DishMeta();
        
		try {
//			dbconn = GoodFoodServlet.DS.getConnection();
			dbconn = GoodFoodDishServlet.DS.getConnection();
			Statement stmt = null;
			
			stmt = dbconn.createStatement();
			ResultSet rs = stmt.executeQuery(select_biz);
			while(rs.next()){
				dishMeta.dishID = rs.getString("id");
				dishMeta.dishText = rs.getString("foodText");
				dishMeta.bizSrcID = rs.getString("bizSrcID");
				dishMeta.dataSource = rs.getInt("dataSource");
				dishMeta.bizName = rs.getString("bizName");
				dishMeta.address = rs.getString("address");
				dishMeta.latitude = rs.getString("latitude");
				dishMeta.longitude = rs.getString("longitude");			
				dishMeta.phoneNum = rs.getString("phoneNum");
				dishMeta.bizWebsite = rs.getString("bizWebsite");
				dishMeta.updateTime = rs.getString("updateTime");
				
				break;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			DBConnector.close(dbconn);
			DBConnector.close(psSelectBiz);			
		}
		
//		System.out.println("DISHMETA1" + dishMeta);
		
		String jsonStr = null;
		
		//TODO ugly code, should be changed

			if(pretty){
				Gson gson = new GsonBuilder().setPrettyPrinting().create();
				jsonStr = gson.toJson(dishMeta);
//				System.out.println("PRETTY\t" + jsonStr);
			}else{
//				System.out.println(dishMeta.bizName + dishMeta.address);
//				Gson gson = new Gson();
				jsonStr = gson.toJson(dishMeta);
//				System.out.println("GSON instance\t" + gson);
//				System.out.println("GSON instance\t" + gson.toString());
//				System.out.println("NONPRETTY\t" + jsonStr);

			}	
			
		return jsonStr; 
	}
	
	
	/**
	 * handle request, call processor to get and process info accordingly
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		if(args.length < 1){
			System.out.println("no property file input!!!");
			System.exit(0);
		}
		List<String[]> locations = new ArrayList<String[]>();
		locations.add(new String[]{"37.6141019893", "-122.395812287"});
		locations.add(new String[]{"33.9388100309", "-118.402768344"});
		locations.add(new String[]{"40.6929648399", "-74.1845529215"});
		locations.add(new String[]{"39.1792298757", "-76.6744909797"});

		RequestDishProcessor handler = new RequestDishProcessor(args[0]);
		for(String[] loc : locations){
			long startTime = System.currentTimeMillis();
//			handler.callProcessors(loc[0], loc[1]);
			long endTime = System.currentTimeMillis();
			System.out.println("running time:\t" + ( endTime - startTime));
		}
	}

}
