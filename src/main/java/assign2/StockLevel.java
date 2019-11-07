package assign2;

import java.util.ArrayList;
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


public class StockLevel extends Transaction {
	
	public StockLevel(String database, MongoClient client, ReadConcern readConcern, WriteConcern writeConcern) {
		super(database, client, readConcern, writeConcern);
	}

	@Override
	public void process(String[] args) {
		System.out.println("-------- Stock-Level --------");
		int w_id = Integer.parseInt(args[1]);
		int d_id = Integer.parseInt(args[2]);
		int t = Integer.parseInt(args[3]);
		int l = Integer.parseInt(args[4]);
		
		Document district = getDistrict(w_id, d_id);
		int d_next_o_id = district.getInteger("D_NEXT_O_ID");
		int gt_o_id = d_next_o_id - l - 1;
		MongoIterable<Document> orderLines = getOrderLines(w_id, d_id, gt_o_id, d_next_o_id);

		Set<Integer> items = new HashSet<Integer>();
		for (Document orderLine: orderLines) {
				int i_id = orderLine.getInteger("OL_I_ID");
				items.add(i_id);
		}
		
		int count = countStock(w_id, items, t);
		
		System.out.printf(
				"W_ID: %d\n" +
				"NUMBER OF ITEMS S_QUANTITY < %d: %d\n",
				w_id, t, count
		);
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
	
	private MongoIterable<Document> getOrderLines(int w_id, int d_id, int gt_o_id, int d_next_o_id) {
		MongoIterable<Document> cursor = getCollection("orderline")
				.find(and(eq("OL_W_ID", w_id), eq("OL_D_ID", d_id), gt("OL_O_ID", gt_o_id), lt("OL_O_ID", d_next_o_id)))
				.projection(fields(include("OL_I_ID"), excludeId()));
		return cursor;
	}

	private int countStock(int w_id, Set<Integer> items, int t) {
		List<BasicDBObject> query = new ArrayList<BasicDBObject>();	
		query.add(new BasicDBObject("$match", and(eq("S_W_ID", w_id), in("S_I_ID", items), lt("S_QUANTITY", t))));
		BasicDBObject count = new BasicDBObject("_id", "").append("count", new BasicDBObject("$sum", 1));
		query.add(new BasicDBObject("$group", count));
		Document doc = getCollection("stock").aggregate(query).first();
		if (doc == null) {
			return 0;
		}
		return doc.getInteger("count");
	}

}
