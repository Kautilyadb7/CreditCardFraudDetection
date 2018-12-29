package com.upgrad.CapstoneProject;

import java.io.IOException;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


/*Set up a connection to Hbase
 * From the lookup table in Hbase , get the information for a certain member  
 * Lookup table in Hbase has following columns - card_id , ucl , postcode , transaction_dt , score
 * The data coming in from Kafka has new transactions. Pick each one and validate those 
 * Post validation , update the relevant tables in HBase so that the POS executives have access to most recent information always
 */

public class NoSQLDBConAndUpdates implements Serializable {


	private static final long serialVersionUID = 1L;
	private static Admin hBaseAdmin = null;
	public static String ec2PublicIP = null;


	/*
	 * This is the method for setting up the Hbase connection
	 * ec2PublicIP is a static variable which is passed by the user
	 * This is initialized by main method of KafkaStreamingIngestion 
	 */
	public static Admin getHbaseAdmin() throws IOException {


		Configuration conf = HBaseConfiguration.create();
		System.out.println("Connecting to the HBASE server...");
		conf.setInt("timeout", 120000);
		conf.set("hbase.master", ec2PublicIP+":60000");
		conf.set("hbase.zookeeper.quorum",ec2PublicIP);
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent","/hbase");
		try {
			Connection con = ConnectionFactory.createConnection(conf);
			if (hBaseAdmin == null) {
				hBaseAdmin = con.getAdmin();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		return hBaseAdmin;

	}

	//Below method looks up the score for input card_id from lookup_hive HBASE table
	public static Integer getScore(String cardID) throws IOException {

		if(cardID == null) {
			System.out.println("The transaction data recieved is null...Kindly check the Kafka stream. ");
			return -1;
		}else {

			//Call the getHbaseAdmin() method only if connection is not already established
			if(hBaseAdmin == null) {
				hBaseAdmin = getHbaseAdmin();
			}

			//System.out.println("Connecting HBase now to get score for the input card_id...");
			String table = "lookup_hive";
			int val = 0;
			try {
				Get g = new Get(Bytes.toBytes(cardID));
				Table htable = hBaseAdmin.getConnection().getTable(TableName.valueOf(table));
				Result result = htable.get(g);
				byte[] value = result.getValue(Bytes.toBytes("ctfamily"), Bytes.toBytes("score"));
				if (value != null) {
					val = Integer.parseInt(Bytes.toString(value));
				}else val = 0;

			} catch (Exception e) {
				e.printStackTrace();
			} 

			return val;
		}
	}

	//Below method looks up the upper control limit for input card_id from lookup_hive HBASE table
	public static Double getUCL(String cardID) throws IOException {

		if(cardID == null) {
			System.out.println("The transaction data recieved is null...Kindly check the Kafka stream. ");
			return -1d;
		}else {

			//Call the getHbaseAdmin() method only if connection is not already established
			if(hBaseAdmin == null) {
				hBaseAdmin = getHbaseAdmin();
			}

			//System.out.println("Connecting HBase now to get ucl for the input card_id...");
			String table = "lookup_hive";
			double val = 0d;
			try {
				Get g = new Get(Bytes.toBytes(cardID));
				Table htable = hBaseAdmin.getConnection().getTable(TableName.valueOf(table));
				Result result = htable.get(g);
				byte[] value = result.getValue(Bytes.toBytes("ctfamily"), Bytes.toBytes("ucl"));
				if (value != null) {
					val = Double.parseDouble(Bytes.toString(value));
				}else val = 0d;

			} catch (Exception e) {
				e.printStackTrace();
			} 

			return val;
		}
	}

	//Below method looks up the post code for input card_id's last genuine transaction from lookup_hive HBASE table
	public static String getPostCode(String cardID) throws IOException {

		if(cardID == null) {
			System.out.println("The transaction data recieved is null...Kindly check the Kafka stream. ");
			return null;
		}else {

			//Call the getHbaseAdmin() method only if connection is not already established
			if(hBaseAdmin == null) {
				hBaseAdmin = getHbaseAdmin();
			}
			//System.out.println("Connecting HBase now to get last postcode for the input card_id...");
			String table = "lookup_hive";
			String val = null;
			try {
				Get g = new Get(Bytes.toBytes(cardID));
				Table htable = hBaseAdmin.getConnection().getTable(TableName.valueOf(table));
				Result result = htable.get(g);
				byte[] value = result.getValue(Bytes.toBytes("ctfamily"), Bytes.toBytes("postcode"));
				if (value != null) {
					val = Bytes.toString(value);
				}else val = null;

			} catch (Exception e) {
				e.printStackTrace();
			}

			return val;
		}
	}

	//Below method looks up the transaction date for input card_id's last genuine transaction from lookup_hive HBASE table
	public static Date getTransactionDt(String cardID) throws IOException {

		if(cardID == null) {
			System.out.println("The transaction data recieved is null...Kindly check the Kafka stream. ");
			return null;
		}else {

			//Call the getHbaseAdmin() method only if connection is not already established
			if(hBaseAdmin == null) {
				hBaseAdmin = getHbaseAdmin();
			}

			//System.out.println("Connecting HBase now to get last trasaction date for the input card_id...");
			String table = "lookup_hive";
			Date val = null;
			try {
				Get g = new Get(Bytes.toBytes(cardID));
				Table htable = hBaseAdmin.getConnection().getTable(TableName.valueOf(table));
				Result result = htable.get(g);
				byte[] value = result.getValue(Bytes.toBytes("ctfamily"), Bytes.toBytes("transaction_dt"));
				if (value != null) {
					val = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss")
							.parse(Bytes.toString(value));
				}else val = null;

			} catch (Exception e) {
				e.printStackTrace();
			} 
			return val;
		}
	}

	/*
	 * Below method performs 2 tasks -
	 * 1. It inserts the incoming transaction details in the transactions_hive HBASE table.
	 * 2. It additionally updates the lookup_hive HBASE table with the current incoming post code and transaction date
	 * if and only if the status of current transaction is GENUINE.
	 */
	public static void updateTable(FraudDetection transactionData) throws IOException {

		try {
			if( "".equals(transactionData.getMember_id()) || transactionData.getMember_id().equals(null) || 
					Double.valueOf(transactionData.getAmount()).equals(null) || transactionData.getTransaction_dt() == null ) {

				System.out.println("One of the values in composite key - member_id  or amount or transaction_dt is blank or null. "
						+ "Moving to next record..");

			}else {

				//Call the getHbaseAdmin() method only if connection is not already established
				if(hBaseAdmin == null) {
					hBaseAdmin = getHbaseAdmin();
				}

				//Converting the incoming object date to specified format
				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				String val = dateFormat.format(transactionData.getTransaction_dt());

				//System.out.println("Connecting HBase now to update the transaction record in transactions table.....");
				String tblName1 = "transactions_hive";

				// Check if table exists
				if (hBaseAdmin.tableExists(TableName.valueOf(tblName1))) {
					Table htable = hBaseAdmin.getConnection().getTable(TableName.valueOf(tblName1));

					/*********** adding a new row ***********/

					/*
					 * Since I have created a composite row key for transactions_hive table,
					 * the key is created as - member_id,amount,transaction_dt
					 * 
					 */

					Put p = new Put(Bytes.toBytes(transactionData.getMember_id() + "_" +
							transactionData.getAmount() + "_" + val));


					//Adding other column values in each column
					p.addColumn(Bytes.toBytes("ctfamily"), Bytes.toBytes("card_id"), Bytes.toBytes(transactionData.getCard_id()));
					p.addColumn(Bytes.toBytes("ctfamily"), Bytes.toBytes("postcode"), Bytes.toBytes(transactionData.getPostcode()));
					p.addColumn(Bytes.toBytes("ctfamily"), Bytes.toBytes("pos_id"), Bytes.toBytes(transactionData.getPos_id()));
					p.addColumn(Bytes.toBytes("ctfamily"), Bytes.toBytes("status"), Bytes.toBytes(transactionData.getStatus()));
					htable.put(p);
					System.out.println("Hbase Table '" + tblName1 + "' is Populated");
				}
				else {
					System.out.println("The HBase Table named '" + tblName1 + "' doesn't exists.");
				}
				//Checking if status was Genuine, send it for updating the lookup_hive table
				if(transactionData.getStatus().equals("GENUINE")) {
					System.out.println("Since the transaction is GENUINE, hence updating the lookup table with current details...");
					updateLookUpTable(transactionData.getCard_id() , 
							transactionData.getPostcode() , transactionData.getTransaction_dt());
				}

			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * Below method updates the lookup_hive HBASE table with the current incoming post code and transaction date
	 * for a specific card_id
	 */
	private static void updateLookUpTable(String card_id , String latestPostCode , Date latestTransactionDt) throws IOException {

		try {

			//Converting the incoming object date to specified format
			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String val = dateFormat.format(latestTransactionDt);

			//Call the getHbaseAdmin() method only if connection is not already established
			if(hBaseAdmin == null) {
				hBaseAdmin = getHbaseAdmin();
			}

			//System.out.println("Connecting HBase now for updating lookup_hive table with latest post code and transaction date...");
			String table = "lookup_hive";


			// Check if table exists
			if (hBaseAdmin.tableExists(TableName.valueOf(table))) {
				Table htable = hBaseAdmin.getConnection().getTable(TableName.valueOf(table));

				//Instantiating the Put class to row name. "card_id" is the row key in lookup_hive table
				Put p = new Put(Bytes.toBytes(card_id));

				//Updating the cell values
				p.addColumn(Bytes.toBytes("ctfamily"), Bytes.toBytes("postcode"), Bytes.toBytes(latestPostCode));
				p.addColumn(Bytes.toBytes("ctfamily"), Bytes.toBytes("transaction_dt"), Bytes.toBytes(val));

				// Saving the put Instance to the HTable.
				htable.put(p);
				System.out.println("Postcode and TransactionDt updated for card id :  '"+ card_id + "' in lookup_hive HBase table");

			}else {
				System.out.println("The HBase Table named '" + table + "' doesn't exists.");
			}
		}catch(Exception e) {
			e.printStackTrace();
		}

	}

	//Below method takes care of closing the HBase connection
	public static void closeConnection() throws IOException {
		System.out.println("Closing the connection now.....");
		hBaseAdmin.getConnection().close();
		System.out.println("Exiting Program");

	}


}
