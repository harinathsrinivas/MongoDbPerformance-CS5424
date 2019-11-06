package assign2;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClients;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;

public class LoadData {
	public static final String DATABASE = "wholesale";
	private static final String DEFAULT_MONGOIMPORT_PATH = "/temp/MongoDb/mongo/mongos/mongodb-linux-x86_64-rhel70-4.2.1/bin/mongoimport";
	private static final String DEFAULT_DATA_PATH = "project-files/data-files/";
	private static final String HOST = "192.168.56.159";
	private static final String NUM_WORKERS = "24";

	
	//private static final String DEFAULT_MONGOIMPORT_PATH = "/usr/local/bin/mongoimport";
	//private static final String HOST = "localhost";
	
	private String mongoimportPath;
	private String dataPath;
	private MongoClient client;
	private MongoDatabase db;

	public static void main(String[] args) {
		LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
		Logger rootLogger = loggerContext.getLogger("org.mongodb.driver");
		rootLogger.setLevel(Level.OFF);

		LoadData loader = new LoadData(DEFAULT_MONGOIMPORT_PATH, DEFAULT_DATA_PATH);
		loader.start();
		loader.getDatabase().drop();
		loader.enableSharding();
		loader.loadWarehouseData();
		loader.loadDistrictData();
		loader.loadCustomerData();
		loader.loadOrderData();
		loader.loadItemData();
		loader.loadOrderLineData();
		loader.loadStockData();
		loader.close();
	}
	
	public LoadData(String mongoimportPath, String dataPath) {
		this.mongoimportPath = mongoimportPath;
		this.dataPath = dataPath;
	}
	
	public void start() {
		client = MongoClients.create("mongodb://" + HOST);
		db = client.getDatabase(DATABASE);
	}
	
	public void close() {
		client.close();
	}
	
	public MongoDatabase getDatabase() {
		return db;
	}

	public void loadWarehouseData() {
		String name = "warehouse";
		String[] columnNames = {"W_ID.int32()", "W_NAME.string()", "W_STREET_1.string()", "W_STREET_2.string()", "W_CITY.string()", 
				                "W_STATE.string()", "W_ZIP.string()", "W_TAX.decimal()", "W_YTD.decimal()"};
		String[] keys = {"W_ID"};
		db.getCollection(name).createIndex(Indexes.ascending(keys), new IndexOptions().unique(true));
		setShardKey(name, keys);
		loadFromCsv(name, columnNames);
	}
	
	public void loadDistrictData() {
		String name = "district";
		String[] columnNames = {"D_W_ID.int32()", "D_ID.int32()", "D_NAME.string()", "D_STREET_1.string()", "D_STREET_2.string()", 
								"D_CITY.string()", "D_STATE.string()", "D_ZIP.string()", "D_TAX.decimal()", "D_YTD.decimal()", "D_NEXT_O_ID.int32()"};
		String[] keys = {"D_W_ID", "D_ID"};
		db.getCollection(name).createIndex(Indexes.ascending(keys), new IndexOptions().unique(true));
		setShardKey(name, keys);
		loadFromCsv(name, columnNames);
	}
	
	public void loadCustomerData() {
		String name = "customer";
		String[] columnNames = {"C_W_ID.int32()", "C_D_ID.int32()", "C_ID.int32()", "C_FIRST.string()", "C_MIDDLE.string()", 
								"C_LAST.string()", "C_STREET_1.string()", "C_STREET_2.string()", "C_CITY.string()", "C_STATE.string()", 
								"C_ZIP.string()", "C_PHONE.string()", "C_SINCE.date(2006-01-02 15:04:05.999)", "C_CREDIT.string()", "C_CREDIT_LIM.decimal()",
								"C_DISCOUNT.decimal()", "C_BALANCE.decimal()", "C_YTD_PAYMENT.double()", "C_PAYMENT_CNT.int32()",
								"C_DELIVERY_CNT.int32()", "C_DATA.string()"};
		String[] keys = {"C_W_ID", "C_D_ID", "C_ID"};
		db.getCollection(name).createIndex(Indexes.ascending(keys), new IndexOptions().unique(true));
		db.getCollection(name).createIndex(Indexes.descending("C_BALANCE"));
		setShardKey(name, keys);
		loadFromCsv(name, columnNames);
	}
    
	public void loadOrderData() {
		String name = "order";
		String[] columnNames = {"O_W_ID.int32()", "O_D_ID.int32()", "O_ID.int32()", "O_C_ID.int32()", "O_CARRIER_ID.int32()", 
								"O_OL_CNT.int32()", "O_ALL_LOCAL.boolean()", "O_ENTRY_D.date(2006-01-02 15:04:05.999)"};
		String[] keys = {"O_W_ID", "O_D_ID", "O_ID"};
		db.getCollection(name).createIndex(Indexes.ascending(keys), new IndexOptions().unique(true));
		//db.getCollection(name).createIndex(Indexes.ascending("O_W_ID", "O_D_ID", "O_CARRIER_ID"));
		//db.getCollection(name).createIndex(Indexes.compoundIndex(Indexes.ascending("O_W_ID", "O_D_ID", "O_C_ID"), Indexes.descending("O_ID")));
		setShardKey(name, keys);
		loadFromCsv(name, columnNames, "-1");
	}
	
	public void loadItemData() {
		String name = "item";
		String[] columnNames = {"I_ID.int32()", "I_NAME.string()", "I_PRICE.decimal()", "I_IM_ID.int32()", "I_DATA.string()"};
		String[] keys = {"I_ID"};
		db.getCollection(name).createIndex(Indexes.ascending(keys), new IndexOptions().unique(true));
		setShardKey(name, keys);
		loadFromCsv(name, columnNames);
	}
    
	public void loadOrderLineData() {
		String name = "orderline";
		String[] columnNames = {"OL_W_ID.int32()", "OL_D_ID.int32()", "OL_O_ID.int32()", "OL_NUMBER.int32()", "OL_I_ID.int32()", 
								"OL_DELIVERY_D.date(2006-01-02 15:04:05.999)", "OL_AMOUNT.decimal()", "OL_SUPPLY_W_ID.int32()", "OL_QUANTITY.decimal()",
								"OL_DIST_INFO.string()"};
		String[] keys = {"OL_W_ID", "OL_D_ID", "OL_O_ID", "OL_NUMBER"};
		db.getCollection(name).createIndex(Indexes.ascending(keys), new IndexOptions().unique(true));
		//db.getCollection(name).createIndex(Indexes.ascending("OL_W_ID", "OL_I_ID"));
		setShardKey(name, keys);
		loadFromCsv("order-line", columnNames, "");
	}

	public void loadStockData() {
		String name = "stock";
		String[] columnNames = {"S_W_ID.int32()", "S_I_ID.int32()", "S_QUANTITY.decimal()", "S_YTD.decimal()", "S_ORDER_CNT.int32()", 
								"S_REMOTE_CNT.int32()", "S_DIST_01.string()", "S_DIST_02.string()", "S_DIST_03.string()",
								"S_DIST_04.string()", "S_DIST_05.string()", "S_DIST_06.string()", "S_DIST_07.string()",
								"S_DIST_08.string()", "S_DIST_09.string()", "S_DIST_10.string()", "S_DATA.string()"};
		String[] keys = {"S_W_ID", "S_I_ID"};
		db.getCollection(name).createIndex(Indexes.ascending(keys), new IndexOptions().unique(true));
		setShardKey(name, keys);
		loadFromCsv(name, columnNames);
	}

	private void loadFromCsv(String collectionName, String[] fieldNames) {
		String filepath = dataPath + collectionName + ".csv";
		String fields = "\"" + String.join(",", fieldNames) + "\"";
		String[] cmd = {mongoimportPath, 
						"--host", HOST,
				        "-d", DATABASE, 
				        "-c", collectionName.replace("-", ""), 
				        "--type", "csv", 
				        "--file", filepath,
				        "--fields", fields,
				        "--numInsertionWorkers", NUM_WORKERS,
				        "--columnsHaveTypes"};
		executeCmd(cmd);
	}
	
	private void loadFromCsv(String collectionName, String[] fieldNames, String replaceNull) {
		String filepath = dataPath + collectionName + ".csv";
		String sedCmd = "sed s/,null,/," + replaceNull + ",/ " + filepath;
		String tmppath = "/tmp/mongotmp.csv";
		String[] sed = {"/bin/sh", "-c", sedCmd + " > " + tmppath};
		executeCmd(sed);
		
		String fields = "\"" + String.join(",", fieldNames) + "\"";
		String[] importCmd = {mongoimportPath,
							  "--host", HOST,
					          "-d", DATABASE, 
					          "-c", collectionName.replace("-", ""), 
					          "--type", "csv", 
					          "--file", tmppath,
					          "--fields", fields,
					          "--numInsertionWorkers", NUM_WORKERS,
					          "--columnsHaveTypes",
					          "--ignoreBlanks"};
		//String[] cmd = {"/bin/sh", "-c", sedCmd + " | " + String.join(" ", importCmd)};
		executeCmd(importCmd);
		
		String[] rm = {"/bin/rm", "-rf", tmppath};
		executeCmd(rm);
	}
	
	private void executeCmd(String[] cmd) {
		try {
			Process p = Runtime.getRuntime().exec(cmd);
			BufferedReader stderr = new BufferedReader(new InputStreamReader(p.getErrorStream())); 
			String s = ""; 
			while ((s = stderr.readLine()) != null) { 
			     System.out.println(s); 
			} 
		}
		catch (Exception e) { 
			e.printStackTrace();
		} 
	}
	
	private void setShardKey(String collection, String... fields) {
		try {
			BasicDBObject keys = new BasicDBObject();
			for (String field : fields) {
				keys = keys.append(field, 1);
			}
			BasicDBObject cmd = new BasicDBObject("shardCollection", DATABASE + "." + collection).
					  append("key", keys).append("unique", true);
			Document result = client.getDatabase("admin").runCommand((Bson) cmd);
			System.out.println(result.toJson());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void enableSharding() {
		try {
			Document result = client.getDatabase("admin").runCommand(new BasicDBObject("enableSharding", DATABASE));
			System.out.println(result.toJson());
		} catch (Exception e) {
			e.printStackTrace();
		}		
	}
}
