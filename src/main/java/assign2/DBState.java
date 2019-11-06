package assign2;

import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.ReadConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;

public class DBState {
	private static final String DATABASE = LoadData.DATABASE;

	public static void main(String[] args) {
		LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
		Logger rootLogger = loggerContext.getLogger("org.mongodb.driver");
		rootLogger.setLevel(Level.OFF);

		String connectionString = args[0];
		MongoClient client = MongoClients.create(connectionString);
		DBState state = new DBState(client);
		
		state.getWarehouseSummary();
		state.getDistrictSummary();
		state.getCustomerSummary();
		state.getOrderSummary();
		state.getOrderLineSummary();
		state.getStockSummary();
		client.close();
	}

	private MongoClient client;
	
	public DBState(MongoClient client) {
		this.client = client;
	}
	
	public void getWarehouseSummary() {
		String[] fields = {"W_YTD"};
		collectionSummary("warehouse", "sum", fields);
	}
	
	public void getDistrictSummary() {
		String[] fields = {"D_YTD", "D_NEXT_O_ID"};
		collectionSummary("district", "sum", fields);
	}
	
	public void getCustomerSummary() {
		String[] fields = {"C_BALANCE", "C_YTD_PAYMENT", "C_PAYMENT_CNT", "C_DELIVERY_CNT"};
		collectionSummary("customer", "sum", fields);
	}
	
	public void getOrderSummary() {
		String[] fields = {"O_ID"};
		collectionSummary("order", "max", fields);
		String[] fields2 = {"O_OL_CNT"};
		collectionSummary("order", "sum", fields2);
	}
	
	public void getOrderLineSummary() {
		String[] fields = {"OL_AMOUNT", "OL_QUANTITY"};
		collectionSummary("orderline", "sum", fields);
	}
	
	public void getStockSummary() {
		String[] fields = {"S_QUANTITY", "S_YTD", "S_ORDER_CNT", "S_REMOTE_CNT"};
		collectionSummary("stock", "sum", fields);
	}
	
	private MongoCollection<Document> getCollection(String collectionName) {
		return client.getDatabase(DATABASE).withReadConcern(ReadConcern.MAJORITY)
										   .getCollection(collectionName);
	}
	
	private void collectionSummary(String collection, String agg, String... fields) {
		List<Bson> query = new ArrayList<Bson>();
		
		BasicDBObject sums = new BasicDBObject("_id", "");
		for (String field : fields) {
			sums = sums.append(field, new BasicDBObject("$" + agg, "$" + field));
		}
		query.add(new BasicDBObject("$group", sums));
		Document doc = getCollection(collection).aggregate(query).first();
		if (doc == null) {
			throw new IllegalArgumentException("error");
		}
		for (String field : fields) {
			try {
				try {
					System.out.println(String.format("%s(%s): %.2f", agg, field, doc.get(field, Decimal128.class).bigDecimalValue()));
				} catch (Exception e) {
					System.out.println(String.format("%s(%s): %.2f", agg, field, doc.get(field, Double.class)));
				}
			} catch (Exception e) {
				System.out.println(String.format("%s(%s): %d", agg, field, doc.getInteger(field)));
			}
			
		}
	}
}
