package assign2;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;

import com.mongodb.BasicDBObject;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.result.UpdateResult;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Updates.*;

public class Delivery extends Transaction {
	
	public Delivery(String database, MongoClient client, ReadConcern readConcern, WriteConcern writeConcern) {
		super(database, client, readConcern, writeConcern);
	}

	@Override
	public void process(String[] args) {
		System.out.println("-------- Delivery --------");
		int w_id = Integer.parseInt(args[1]);
		int o_carrier_id = Integer.parseInt(args[2]);
		for (int d_id = 1; d_id <= 10; d_id++) {
			Document order;
			UpdateResult update;
			int o_id;
			do {
				order = getOrder(w_id, d_id);
				if (order == null)
					break;
				o_id = order.getInteger("O_ID");
				update = updateOrder(w_id, d_id, o_id, o_carrier_id);
			} while (update.getModifiedCount() == 0);
			if (order == null)
				continue; // all orders are delivered
			o_id = order.getInteger("O_ID");
			updateOrderLines(w_id, d_id, o_id);
			int c_id = order.getInteger("O_C_ID");
			BigDecimal totalAmount = getTotalAmount(w_id, d_id, o_id);
			if (totalAmount != null) {
				updateCustomer(w_id, d_id, c_id, totalAmount);
			}
		}
	}

	private Document getOrder(int w_id, int d_id) {		
		Document doc = getCollection("order")
				.find(and(eq("O_W_ID", w_id), eq("O_D_ID", d_id), eq("O_CARRIER_ID", -1)))
				.projection(fields(include("O_ID", "O_C_ID"), excludeId()))
		        .sort(new BasicDBObject("O_ID", 1))
		        .limit(1)
		        .first();
		return doc;
	}

	private BigDecimal getTotalAmount(int w_id, int d_id, int o_id) {
		List<Bson> query = new ArrayList<Bson>();
		query.add(new BasicDBObject("$match", and(eq("OL_W_ID", w_id), eq("OL_D_ID", d_id), eq("OL_O_ID", o_id))));
		BasicDBObject sums = new BasicDBObject("_id", "").append("TOTAL_OL_AMOUNT", new BasicDBObject("$sum", "$OL_AMOUNT"));
		query.add(new BasicDBObject("$group", sums));
		Document doc = getCollection("orderline").aggregate(query).first();
		if (doc == null) {
			return null;
		}
		return doc.get("TOTAL_OL_AMOUNT", Decimal128.class).bigDecimalValue();
	}

	private UpdateResult updateOrder(int w_id, int d_id, int o_id, int o_carrier_id) {
		UpdateResult result = getCollection("order").updateOne(
				and(eq("O_W_ID", w_id), eq("O_D_ID", d_id), eq("O_ID", o_id), eq("O_CARRIER_ID", -1)),
				set("O_CARRIER_ID", o_carrier_id));
		return result;
	}
	
	private UpdateResult updateOrderLines(int w_id, int d_id, int o_id) {
		Date ol_delivery_d = new Date();
		UpdateResult result = getCollection("orderline").updateMany(
				and(eq("OL_W_ID", w_id), eq("OL_D_ID", d_id), eq("OL_O_ID", o_id)),
				set("OL_DELIVERY_D", ol_delivery_d));
		return result;
	}

	private UpdateResult updateCustomer(int w_id, int d_id, int c_id, BigDecimal totalAmount) {
		UpdateResult result = getCollection("customer").updateOne(
				and(eq("C_W_ID", w_id), eq("C_D_ID", d_id), eq("C_ID", c_id)),
				combine(inc("C_BALANCE", totalAmount), inc("C_DELIVERY_CNT", 1)));
		return result;
	}
}
