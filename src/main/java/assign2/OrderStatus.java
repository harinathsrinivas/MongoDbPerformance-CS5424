package assign2;

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

public class OrderStatus extends Transaction {

	public static void main(String[] args) {
		Logger.getLogger("org.mongodb.driver").setLevel(Level.SEVERE);

		MongoClient client = MongoClients.create();
		OrderStatus t = new OrderStatus("wholesale", client, ReadConcern.LOCAL, WriteConcern.W1);
		String[] args2 = {"O", "1", "1", "1234"};
		t.process(args2);
		client.close();
	}
	
	public OrderStatus(String database, MongoClient client, ReadConcern readConcern, WriteConcern writeConcern) {
		super(database, client, readConcern, writeConcern);
	}

	@Override
	public void process(String[] args) {
		System.out.println("-------- Order-Status --------");
		int w_id = Integer.parseInt(args[1]);
		int d_id = Integer.parseInt(args[2]);
		int c_id = Integer.parseInt(args[3]);

		Document customer = getCustomer(w_id, d_id, c_id);
		Document order = getOrder(w_id, d_id, c_id);
		int o_id = order.getInteger("O_ID");
		MongoIterable<Document> orderLines = getOrderLines(w_id, d_id, o_id);
		
		System.out.printf(
				"C_NAME: (%s, %s, %s), C_BALANCE: %f\n",
				customer.getString("C_FIRST"), 
				customer.getString("C_MIDDLE"), 
				customer.getString("C_LAST"), 
				((Decimal128) customer.get("C_BALANCE")).bigDecimalValue()
		);
		System.out.printf(
				"O_ID: %d, O_ENTRY_D: %tc, O_CARRIER_ID: %d\n",
				order.getInteger("O_ID"), 
				order.getDate("O_ENTRY_D"),
				order.getInteger("O_CARRIER_ID")
		);
		for (Document orderLine: orderLines) {
			System.out.printf(
					"OL_I_ID: %d, OL_SUPPLY_W_ID: %d, OL_QUANTITY: %f " +
					"OL_AMOUNT: %f, OL_DELIVER_D: %tc\n",
					orderLine.getInteger("OL_I_ID"), 
					orderLine.getInteger("OL_SUPPLY_W_ID"),
					((Decimal128) orderLine.get("OL_QUANTITY")).bigDecimalValue(),
					((Decimal128) orderLine.get("OL_AMOUNT")).bigDecimalValue(),
					orderLine.getDate("OL_DELIVERY_D")
			);
		}
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
	
	private Document getOrder(int w_id, int d_id, int c_id) {
		Document doc = getCollection("order")
				.find(and(eq("O_W_ID", w_id), eq("O_D_ID", d_id), eq("O_C_ID", c_id)))
		        .projection(fields(include("O_ID", "O_ENTRY_D", "O_CARRIER_ID"), excludeId()))
		        .sort(new BasicDBObject("O_ID", -1))
		        .limit(1)
		        .first();
		if (doc == null) {
			throw new IllegalArgumentException("No matching order");
		}
		return doc;
	}
	
	private MongoIterable<Document> getOrderLines(int w_id, int d_id, int o_id) {
		MongoIterable<Document> cursor = getCollection("orderline")
				.find(and(eq("OL_W_ID", w_id), eq("OL_D_ID", d_id), eq("OL_O_ID", o_id)))
		        .projection(fields(include("OL_I_ID", "OL_SUPPLY_W_ID", "OL_QUANTITY", "OL_AMOUNT", "OL_DELIVERY_D"), excludeId()));
		return cursor;
	}

}
