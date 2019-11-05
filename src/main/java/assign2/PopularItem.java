package assign2;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bson.Document;
import org.bson.types.Decimal128;

import com.mongodb.BasicDBObject;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoIterable;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;

public class PopularItem extends Transaction {
	
	public static void main(String[] args) {
		Logger.getLogger("org.mongodb.driver").setLevel(Level.SEVERE);

		MongoClient client = MongoClients.create();
		PopularItem t = new PopularItem("wholesale", client, ReadConcern.LOCAL, WriteConcern.W1);
		String[] args2 = {"O", "1", "1", "20"};
		t.process(args2);
		client.close();
	}

	public PopularItem(String database, MongoClient client, ReadConcern readConcern, WriteConcern writeConcern) {
		super(database, client, readConcern, writeConcern);
	}

	@Override
	public void process(String[] args) {
		System.out.println("-------- Popular-Item --------");
		int w_id = Integer.parseInt(args[1]);
		int d_id = Integer.parseInt(args[2]);
		int l = Integer.parseInt(args[3]);
		
		Document district = getDistrict(w_id, d_id);
		int d_next_o_id = district.getInteger("D_NEXT_O_ID");
		int gt_o_id = d_next_o_id - l - 1;
		MongoIterable<Document> orders = getOrders(w_id, d_id, gt_o_id, d_next_o_id);
		
		System.out.printf(
				"(W_ID, D_ID): (%d, %d)\n" +
				"L: %d\n",
				w_id, d_id, l
		);
		
		Map<String, Integer> items = new HashMap<String, Integer>();
		for (Document order: orders) {
			int o_id = order.getInteger("O_ID");
			int c_id = order.getInteger("O_C_ID");
			Document customer = getCustomer(w_id, d_id, c_id);
			
			System.out.printf(
					"O_ID: %d, O_ENTRY_D: %tc, C_NAME: (%s, %s, %s)\n",
					o_id, 
					order.getDate("O_ENTRY_D"), 
					customer.getString("C_FIRST"), 
					customer.getString("C_MIDDLE"), 
					customer.getString("C_LAST")
			);
			
			MongoIterable<Document> orderLines = getMaxOrderLines(w_id, d_id, o_id);
			for (Document orderLine: orderLines) {
					int i_id = orderLine.getInteger("OL_I_ID");
					Document item = getItem(i_id);
					String i_name = item.getString("I_NAME");
					int count = items.containsKey(i_name) ? items.get(i_name) : 0;
					items.put(i_name, count + 1);
					
					System.out.printf(
							"I_NAME: %s, OL_QUANTITY: %d\n",
							i_name, orderLine.get("OL_QUANTITY", Decimal128.class).intValue()
					);
			}
		}
		
		for (String i_name: items.keySet()) {
			int count = items.get(i_name);
			double percent = (double) count / (double) l * 100.0;
			
			System.out.printf(
					"I_NAME: %s, PERCENT OF ORDERS: %f percent\n",
					i_name, percent
			);
		}
	}

	private Document getDistrict(int w_id, int d_id) {
		Document doc = getCollection("district")
				.find(and(eq("D_W_ID", w_id), eq("D_ID", d_id)))
				.projection(fields(include("D_NEXT_O_ID"), excludeId()))
				.first();
		if (doc == null) {
			throw new IllegalArgumentException("No matching district");
		}
		return doc;
	}

	private MongoIterable<Document> getOrders(int w_id, int d_id, int gt_o_id, int d_next_o_id) {
		MongoIterable<Document> cursor = getCollection("order")
				.find(and(eq("O_W_ID", w_id), eq("O_D_ID", d_id), gt("O_ID", gt_o_id), lt("O_ID", d_next_o_id)))
				.projection(fields(include("O_ID", "O_C_ID", "O_ENTRY_D"), excludeId()));
		return cursor;
	}

	private MongoIterable<Document> getMaxOrderLines(int w_id, int d_id, int o_id) {
		Decimal128 ol_quantity = getMaxQuantity(w_id, d_id, o_id);
		MongoIterable<Document> cursor = getCollection("orderline")
				.find(and(eq("OL_W_ID", w_id), eq("OL_D_ID", d_id), eq("OL_O_ID", o_id), eq("OL_QUANTITY", ol_quantity)))
				.projection(fields(include("OL_I_ID", "OL_QUANTITY"), excludeId()));
		return cursor;
	}
	
	private Decimal128 getMaxQuantity(int w_id, int d_id, int o_id) {
		Document doc = getCollection("orderline")
				.find(and(eq("OL_W_ID", w_id), eq("OL_D_ID", d_id), eq("OL_O_ID", o_id)))
		        .projection(fields(include("OL_QUANTITY"), excludeId()))
		        .sort(new BasicDBObject("OL_QUANTITY", -1))
		        .limit(1)
		        .first();
		if (doc == null) {
			throw new IllegalArgumentException("No matching orderline");
		}
		return doc.get("OL_QUANTITY", Decimal128.class);
	}
	
	private Document getCustomer(int w_id, int d_id, int c_id) {
		Document doc = getCollection("customer")
				.find(and(eq("C_W_ID", w_id), eq("C_D_ID", d_id), eq("C_ID", c_id)))
				.projection(fields(include("C_FIRST", "C_MIDDLE", "C_LAST", "C_BALANCE"), excludeId()))
				.first();
		if (doc == null) {
			throw new IllegalArgumentException("No matching customer");
		}
		return doc;
	}
	
	private Document getItem(int i_id) {
		Document doc = getCollection("item")
				.find(eq("I_ID", i_id))
				.projection(fields(include("I_NAME"), excludeId()))
				.first();
		if (doc == null) {
			throw new IllegalArgumentException("No matching item");
		}
		return doc;
	}
}
