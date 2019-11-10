package assign2;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoIterable;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;

public class RelatedCustomer extends Transaction {

	private static int THRESHOLD = 2;

	public RelatedCustomer(String database, MongoClient client, ReadConcern readConcern, WriteConcern writeConcern) {
		super(database, client, readConcern, writeConcern);
	}

	@Override
	public void process(String[] args) {
		System.out.println("-------- Related-Customer --------");
		int w_id = Integer.parseInt(args[1]);
		int d_id = Integer.parseInt(args[2]);
		int c_id = Integer.parseInt(args[3]);
		
		Set<List<Integer>> relatedCustomers = new HashSet<List<Integer>>();
		MongoIterable<Document> orders = getOrders(w_id, d_id, c_id);
		for (Document order: orders) {
			int o_id = order.getInteger("O_ID");
			MongoIterable<Document> orderLines = getOrderLines(w_id, d_id, o_id);
			List<Integer> items = new ArrayList<Integer>();
			for (Document orderLine: orderLines) {
				items.add(orderLine.getInteger("OL_I_ID"));
			}
			addRelatedCustomers(w_id, items, relatedCustomers);
		}
		
		for (List<Integer> customer: relatedCustomers) {
			System.out.printf(
					"(C_W_ID, C_D_ID, C_ID): (%d, %d, %d)\n",
					customer.get(0), customer.get(1), customer.get(2)
			);
		}
	}

	private Set<List<Integer>> addRelatedCustomers(
			int w_id, List<Integer> items, Set<List<Integer>> relatedCustomers) {
		List<BasicDBObject> query = new ArrayList<BasicDBObject>();	
		query.add(new BasicDBObject("$match", and(in("OL_I_ID", items), ne("OL_W_ID", w_id))));
		String group = "{$group : {_id :{OL_W_ID:\"$OL_W_ID\", OL_D_ID:\"$OL_D_ID\", OL_O_ID:\"$OL_O_ID\"}," +
				                  "count : {$sum : 1}}}";
		query.add(BasicDBObject.parse(group));
		
		MongoIterable<Document> cursor = getCollection("orderline")
				.aggregate(query);
		
		for (Document doc: cursor) {
	    	if (doc.getInteger("count") >= THRESHOLD) {
	    		Document ids = doc.get("_id", Document.class);
	    		int ol_w_id = ids.getInteger("OL_W_ID");
	    		int d_id = ids.getInteger("OL_D_ID");
	    		int o_id = ids.getInteger("OL_O_ID");
	    		int c_id = getCustomerId(ol_w_id, d_id, o_id);
	    		List<Integer> customer = Arrays.asList(ol_w_id, d_id, c_id);
	    		relatedCustomers.add(customer);
	    	}
		}
		return relatedCustomers;
	}

	private MongoIterable<Document> getOrders(int w_id, int d_id, int c_id) {
		MongoIterable<Document> cursor = getCollection("order")
				.find(and(eq("O_W_ID", w_id), eq("O_D_ID", d_id), eq("O_C_ID", c_id)))
				.projection(fields(include("O_ID"), excludeId()));
		return cursor;
	}
	
	private MongoIterable<Document> getOrderLines(int w_id, int d_id, int o_id) {
		MongoIterable<Document> cursor = getCollection("orderline")
				.find(and(eq("OL_W_ID", w_id), eq("OL_D_ID", d_id), eq("OL_O_ID", o_id)))
				.projection(fields(include("OL_I_ID", "OL_NUMBER", "OL_O_ID"), excludeId()));
		return cursor;
	}
	
	private int getCustomerId(int w_id, int d_id, int o_id) {
		Document doc = getCollection("order")
				.find(and(eq("O_W_ID", w_id), eq("O_D_ID", d_id), eq("O_ID", o_id)))
				.projection(fields(include("O_C_ID"), excludeId()))
				.first();
		if (doc == null) {
			throw new IllegalArgumentException("No matching order");
		}
		return doc.getInteger("O_C_ID");
	}
}
