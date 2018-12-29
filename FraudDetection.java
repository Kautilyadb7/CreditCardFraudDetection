package com.upgrad.CapstoneProject;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


@SuppressWarnings("serial")
public class FraudDetection implements java.io.Serializable{

	/**
	 * The member variables for the class are declared below
	 */
	private String card_id 		= null;
	private String member_id 	= null;
	private double amount 		= 0.0;
	private String postcode 	= null;
	private String pos_id 		= null;
	private Date transaction_dt	= null;
	private String status		= null;

	
	//This is the default constructor defined for FraudDetection class
	public FraudDetection() {}

	/*
	 * This is the parameterized constructor for FraudDetection class
	 * It takes the incoming record from DStream as JSON and parse it
	 * This constructor initializes the class member variables to the values derived from parsing the JSON.
	 * 
	 */
	public FraudDetection(String jsonObj){

		JSONParser parser = new JSONParser();
		try {
			JSONObject obj = (JSONObject) parser.parse(jsonObj);

			this.card_id 			= obj.get("card_id") + "";
			this.member_id 			= obj.get("member_id") + "";
			this.amount 			= Double.parseDouble(obj.get("amount") + "");
			this.postcode 			= obj.get("postcode") + "";
			this.pos_id 			= obj.get("pos_id") + "";
			this.transaction_dt 	= new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").parse(obj.get("transaction_dt") + "");
		} catch (ParseException e) {
			
			e.printStackTrace();
		} catch(Exception e) {
			
			e.printStackTrace();
		}
	}


	//The below method validates the incoming transaction at POS as "GENUINE" / "FRAUD"
	private void validateTransaction() throws IOException {

		/*
		 * Now we validate the current transaction as
		 * 1. If the member score is less than 200 , the transaction needs to be rejected.
		 * 2. The transaction amount of current transaction cannot exceed beyond UCL set for this card based on last 10 genuine transactions.
		 * 3. If the speed between 2 transactions is more than 1km/4sec = 0.25km/second , it is Fraud. 
		 */
		
		double member_ucl 	= 0d;
		int member_score	= 0;
		double speed 		= 0d;
		
		
		//Getting the UCL for current card_id
		member_ucl = NoSQLDBConAndUpdates.getUCL(this.getCard_id());
		System.out.println("Upper Comtrol Limit for current card_id is :    " + member_ucl);
		
		
		//getting the score for current card_id
		member_score = NoSQLDBConAndUpdates.getScore(this.card_id);
		System.out.println("Credit score for current card_id is :   " + member_score);

		/*
		 * Each of these 3 conditions need to be mandatorily true for the 
		 * transaction status to be GENUINE
		 *
		 */
		
		speed = getSpeed();
		if((member_score >= 200) && (this.amount <= member_ucl) && (speed <= 0.25)) {
			this.status = "GENUINE";
			System.out.println(" The transaction status is : " + status);
		}
		else{
			this.status = "FRAUD";
			System.out.println(" The transaction is found to be breaching one of the validation rules and the status is : " + status);
		}			 
	}

	//The below method calculates the speed in km/sec
	private double getSpeed() {
		
		double distance		= 0d;
		long timeDifference	= 0L;
		double speed		= 0d;
		
		try {
		// Creating an object of DistanceUtility class 
		DistanceUtility distUtil = new DistanceUtility();  

		//Getting last Post Code of the card_id from Lookup table in HBase
		String lastPostCode =  NoSQLDBConAndUpdates.getPostCode(this.getCard_id());
		System.out.println("lastPostCode :  " + lastPostCode);

		//Getting the last transaction_dt of the card_id from Lookup table in HBase
		Date lastTransactionDt = NoSQLDBConAndUpdates.getTransactionDt(this.getCard_id());
		System.out.println("lastTransactionDt :  " + lastTransactionDt);

		//The distance between 2 zip codes is calculated by following method in DistanceUtility class
		//The distance is in kilometers
		distance = distUtil.getDistanceViaZipCode(lastPostCode , this.getPostcode());
		System.out.println("distance between current and last postcode (in km) is  : " + distance);

		/*The time difference between 2 transaction dates
		 * is calculated as below. The difference is returned in milliseconds
		 * For almost 3000+ transactions, the incoming transaction date < lastTransaction date stored in lookup table
		 * Since, this would result in negative time difference, hence using Absolute function
		 * Dividing by 1000 to get the time in seconds. 
		 */ 
		timeDifference = (java.lang.Math.abs(this.getTransaction_dt().getTime() - lastTransactionDt.getTime())) / 1000 ;
		System.out.println("timeDifference between current and last transaction (in seconds) is  " + timeDifference);

		/*All floating point values (float and double) in an arithmetic operation (+, −, *, /) 
		 * are converted to double type before the arithmetic operation in performed
		 * the speed is being calculated in km/sec.
		 */
		speed = distance / timeDifference ;
		System.out.println("Speed (Distance / Time) :  " + speed + "  km/second ");
		
		}catch(Exception e) {
			
			e.printStackTrace();
		}	
		
		return speed;
	}

	/*
	 * This is the method called by DStream records
	 * It first validates the transaction through validateTransaction() method call
	 * It then calls updateTable() method in HBase DAO class 
	 */
	public void updateHBaseTable(FraudDetection obj){
		
		try {
			validateTransaction();
			NoSQLDBConAndUpdates.updateTable(obj);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	

	//Below are the getter methods for the member variables
	public String getCard_id() {
		return card_id;
	}

	public String getMember_id() {
		return member_id;
	}

	public double getAmount() {
		return amount;
	}

	public String getPostcode() {
		return postcode;
	}

	public String getPos_id() {
		return pos_id;
	}

	public Date getTransaction_dt() {
		return transaction_dt;
	}

	public String getStatus() {
		return status;
	}

}



