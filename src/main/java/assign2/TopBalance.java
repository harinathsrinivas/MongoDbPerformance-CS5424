package assign2;

import org.bson.Document;
import org.bson.types.Decimal128;

import com.mongodb.BasicDBObject;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoIterable;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;

public class TopBalance extends Transaction {

	public TopBalance(String database, MongoClient client, ReadConcern readConcern, WriteConcern writeConcern) {
		super(database, client, readConcern, writeConcern);
	}

	@Override
	public void process(String[] args) {
		System.out.println("-------- Top-Balance --------");
		int TOP_N = 10;
		MongoIterable<Document> customers = getTopCustomers(TOP_N);
		
		for (Document customer: customers) {
			int w_id = customer.getInteger("C_W_ID");
			int d_id = customer.getInteger("C_D_ID");
			Document warehouse = getWarehouse(w_id);
			Document district = getDistrict(w_id, d_id);
			
			System.out.printf(
					"C_NAME: (%s, %s, %s), C_BALANCE: %f, W_NAME: %s, D_NAME: %s\n",
					customer.getString("C_FIRST"), 
					customer.getString("C_MIDDLE"), 
					customer.getString("C_LAST"), 
					customer.get("C_BALANCE", Decimal128.class).bigDecimalValue(),
					warehouse.getString("W_NAME"),
					district.getString("D_NAME")
			);
		}
	}
	
	private MongoIterable<Document> getTopCustomers(int top_n) {
		MongoIterable<Document> cursor = getCollection("customer")
				.find()
				.projection(fields(include("C_W_ID", "C_D_ID", "C_FIRST", "C_MIDDLE", "C_LAST", "C_BALANCE"), excludeId()))
		        .sort(new BasicDBObject("C_BALANCE", -1))
		        .limit(top_n);
		return cursor;
	}
	
	private Document getWarehouse(int w_id) {
		Document doc = getCollection("warehouse")
				.find(eq("W_ID", w_id))
				.projection(fields(include("W_NAME"), excludeId()))
				.first();
		if (doc == null) {
			throw new IllegalArgumentException("No matching warehouse");
		}
		return doc;
	}
	
	private Document getDistrict(int w_id, int d_id) {
		Document doc = getCollection("district")
				.find(and(eq("D_W_ID", w_id), eq("D_ID", d_id)))
				.projection(fields(include("D_NAME"), excludeId()))
				.first();
		if (doc == null) {
			throw new IllegalArgumentException("No matching district");
		}
		return doc;
	}
}
