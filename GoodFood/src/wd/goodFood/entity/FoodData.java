package wd.goodFood.entity;

import java.util.List;

/**
 * only for json response
 * */
public class FoodData {

	public String foodText;
	public int freq;
	public List<ReviewData> reviews;
	public String bizID;//to link to restaurant it belongs to

	public FoodData(String fText, int freq, List<ReviewData> rs, String bizID){
		this.foodText = fText;
		this.freq = freq;
		this.reviews = rs;
		this.bizID = bizID;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
