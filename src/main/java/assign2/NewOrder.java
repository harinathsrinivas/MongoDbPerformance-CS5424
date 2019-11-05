package assign2;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.types.Decimal128;

import com.mongodb.MongoWriteException;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.result.UpdateResult;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Updates.*;

public class NewOrder extends Transaction {

	public NewOrder(String database, MongoClient client, ReadConcern readConcern, WriteConcern writeConcern) {
		super(database, client, readConcern, writeConcern);
	}

	@Override
	public void process(String[] args) {
		System.out.println("-------- New Order --------");
		int c_id = Integer.parseInt(args[1]);
		int w_id = Integer.parseInt(args[2]);
		int d_id = Integer.parseInt(args[3]);
		int o_ol_cnt = Integer.parseInt(args[4]);
		List<OrderLine> orderLines = parseOrderLines(args[5]);
		
		Document district = getDistrict(w_id, d_id);
		Document customer = getCustomer(w_id, d_id, c_id);
		Document warehouse = getWarehouse(w_id);
		
		int o_id = district.getInteger("D_NEXT_O_ID");
		boolean o_all_local = isAllLocal(w_id, orderLines);
		boolean isValidOId;
		do {
			isValidOId = insertOrder(w_id, d_id, o_id, c_id, o_ol_cnt, o_all_local);
			o_id++;
		} while (!isValidOId);
		o_id = o_id - 1;
		int d_next_o_id = o_id + 1;
		updateDNextOId(w_id, d_id, d_next_o_id);
				
		String c_last = customer.getString("C_LAST");
		String c_credit = customer.getString("C_CREDIT");
		BigDecimal c_discount = customer.get("C_DISCOUNT", Decimal128.class).bigDecimalValue();
		System.out.printf(
				"(W_ID, D_ID, C_ID): (%d, %d, %d), C_LAST: %s, C_CREDIT: %s, C_DISCOUNT: %f\n",
				w_id, d_id, c_id, c_last, c_credit, c_discount
		);
		BigDecimal w_tax = warehouse.get("W_TAX", Decimal128.class).bigDecimalValue();
		BigDecimal d_tax = district.get("D_TAX", Decimal128.class).bigDecimalValue();
		System.out.printf("W_TAX: %f, D_TAX: %f\n", w_tax, d_tax);
		
		double totalAmount = insertOrderLines(w_id, d_id, o_id, orderLines);
			
		BigDecimal c_tax = customer.get("C_DISCOUNT", Decimal128.class).bigDecimalValue();
		totalAmount = totalAmount 
				* (1 + w_tax.add(d_tax).doubleValue()) 
				* (1 - c_tax.doubleValue());

		//output
		System.out.printf("NUM_ITEMS: %d, TOTAL_AMOUNT: %f\n", 
				o_ol_cnt, totalAmount);
	}
	
	private List<OrderLine> parseOrderLines(String orderLinesString) {
		List<OrderLine> orderLines = new ArrayList<OrderLine>();
		String[] orderLinesStrArr = orderLinesString.split("\n");
		for (int i = 0; i < orderLinesStrArr.length; i++) {
			String[] args = orderLinesStrArr[i].split(",");
			OrderLine orderLine = new OrderLine(
					i + 1,
					Integer.parseInt(args[0]),
					Integer.parseInt(args[1]),
					new BigDecimal(args[2]));
			orderLines.add(orderLine);
		}
		return orderLines;
	}

	private Document getCustomer(int w_id, int d_id, int c_id) {
		Document doc = getCollection("customer")
				.find(and(eq("C_W_ID", w_id), eq("C_D_ID", d_id), eq("C_ID", c_id)))
				.projection(fields(include("C_LAST", "C_CREDIT", "C_DISCOUNT"), excludeId()))
				.first();
		if (doc == null) {
			throw new IllegalArgumentException("No matching customer");
		}
		return doc;
	}
	
	private Document getWarehouse(int w_id) {
		Document doc = getCollection("warehouse")
				.find(eq("W_ID", w_id))
				.projection(fields(include("W_TAX"), excludeId()))
				.first();
		if (doc == null) {
			throw new IllegalArgumentException("No matching warehouse");
		}
		return doc;
	}
	
	private Document getDistrict(int w_id, int d_id) {
		Document doc = getCollection("district")
				.find(and(eq("D_W_ID", w_id), eq("D_ID", d_id)))
				.projection(fields(include("D_TAX"), include("D_NEXT_O_ID"), excludeId()))
				.first();
		if (doc == null) {
			throw new IllegalArgumentException("No matching district");
		}
		return doc;
	}
	
	private Document getItem(int i_id) {
		Document doc = getCollection("item")
				.find(eq("I_ID", i_id))
				.projection(fields(include("I_NAME"), include("I_PRICE"), excludeId()))
				.first();
		if (doc == null) {
			throw new IllegalArgumentException("No matching item");
		}
		return doc;
	}
	
	private double insertOrderLines(int w_id, int d_id, int o_id, List<OrderLine> orderLines) {
		double totalAmount = 0;
		String ol_dist_info = String.format("S_DIST_%02d", d_id);
		List<Document> orderline_inserts = new ArrayList<Document>();
		for (OrderLine orderLine : orderLines) {
			int ol_supply_w_id = orderLine.getWId();
			int i_id = orderLine.getIId();
			BigDecimal ol_quantity = orderLine.getQuantity();
			
			// update stock info
			boolean is_remote = w_id != ol_supply_w_id;
			BigDecimal s_quantity;
			UpdateResult update;
			do {
				Document stock = getStock(ol_supply_w_id, i_id);
				BigDecimal old_s_quantity = stock.get("S_QUANTITY", Decimal128.class).bigDecimalValue();
				s_quantity = old_s_quantity.add(ol_quantity.negate());
				if (s_quantity.doubleValue() < 10) {
					s_quantity = s_quantity.add(new BigDecimal(100));
				}
				update = updateStock(ol_supply_w_id, i_id, ol_quantity, s_quantity, is_remote, old_s_quantity);
			} while (update.getModifiedCount() == 0);
			
			Document item = getItem(i_id);
			BigDecimal i_price = item.get("I_PRICE", Decimal128.class).bigDecimalValue();
			String i_name = item.getString("I_NAME");
			
			int ol_n = orderLine.getN();
			BigDecimal ol_amount = ol_quantity.multiply(i_price);
			orderline_inserts.add(OrderLineDocument(
					w_id, d_id, o_id, ol_n, i_id, ol_amount,
					ol_supply_w_id, ol_quantity, ol_dist_info));
			
			totalAmount += ol_amount.doubleValue();
			
			System.out.printf(
					"  ITEM_NUMBER: %d, I_NAME: %s, SUPPLIER_WAREHOUSE: %d, QUANTITY: %d, OL_AMOUNT: %f, S_QUANTITY: %d\n",
					i_id, i_name, ol_supply_w_id, ol_quantity.toBigInteger(), ol_amount, s_quantity.toBigInteger()
			);
		}
		
		getCollection("orderline").insertMany(orderline_inserts, new InsertManyOptions().ordered(false));
		return totalAmount;
	}
	
	private Document OrderLineDocument(
			int w_id, int d_id, int o_id, int ol_n, int i_id, BigDecimal ol_amount,
			int ol_supply_w_id, BigDecimal ol_quantity, String ol_dist_info) {
		return new Document().append("OL_W_ID", w_id)
							 .append("OL_D_ID", d_id)
							 .append("OL_O_ID", o_id)
							 .append("OL_NUMBER", ol_n)
							 .append("OL_I_ID", i_id)
							 .append("OL_AMOUNT", ol_amount)
							 .append("OL_SUPPLY_W_ID", ol_supply_w_id)
							 .append("OL_QUANTITY", ol_quantity)
							 .append("OL_DIST_INFO", ol_dist_info);
	}
	
	private Document getStock(int w_id, int i_id) {
		Document doc = getCollection("stock")
				.find(and(eq("S_W_ID", w_id), eq("S_I_ID", i_id)))
				.projection(fields(include("S_QUANTITY"), excludeId()))
				.first();
		if (doc == null) {
			throw new IllegalArgumentException("No matching stock");
		}
		return doc;
	}
	
	private UpdateResult updateStock(
			int w_id, int i_id, BigDecimal ol_quantity, BigDecimal s_quantity, boolean is_remote, BigDecimal old_s_quantity) {
		int s_remote_cnt_inc = is_remote ? 1 : 0;
		UpdateResult result = getCollection("stock").updateOne(
				and(eq("S_W_ID", w_id), eq("S_I_ID", i_id), eq("S_QUANTITY", old_s_quantity)),
				combine(set("S_QUANTITY", s_quantity), 
						inc("S_YTD", ol_quantity), 
						inc("S_ORDER_CNT", 1),
						inc("S_REMOTE_CNT", s_remote_cnt_inc)));
		return result;
	}
	
	private boolean isAllLocal(int w_id, List<OrderLine> orderLines) {
		for (OrderLine orderLine : orderLines) {
			int s_w_id = orderLine.getWId();
			if (w_id == s_w_id) { 
				return false; 
			}
		}
		return true;
	}
	
	private boolean insertOrder(int w_id, int d_id, int o_id, int c_id, int o_ol_cnt, boolean o_all_local) {
		Document doc = new Document().append("O_W_ID", w_id)
									 .append("O_D_ID", d_id)
									 .append("O_ID", o_id)
									 .append("O_C_ID", c_id)
									 .append("O_CARRIER_ID", -1)
									 .append("O_OL_CNT", o_ol_cnt)
									 .append("O_ALL_LOCAL", o_all_local)
									 .append("O_ENTRY_D", new Date());
		try {
			getCollection("order").insertOne(doc);
		} catch (MongoWriteException e) {
			return false;
		}
		return true;
	}
		
	private UpdateResult updateDNextOId(int w_id, int d_id, int d_next_o_id) {
		UpdateResult result = getCollection("district").updateOne(
				and(eq("D_W_ID", w_id), eq("D_ID", d_id)),
				set("D_NEXT_O_ID", d_next_o_id));
		return result;
	}

	private class OrderLine {
		private int n;
		private int i_id;
		private int w_id;
		private BigDecimal quantity;
		
		public OrderLine(int n, int i_id, int w_id, BigDecimal quantity) {
			this.n = n;
			this.i_id = i_id;
			this.w_id = w_id;
			this.quantity = quantity;
		}

		public int getN() {
			return n;
		}

		public int getIId() {
			return i_id;
		}

		public int getWId() {
			return w_id;
		}

		public BigDecimal getQuantity() {
			return quantity;
		}
	}
}
