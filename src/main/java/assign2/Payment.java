package assign2;

import java.math.BigDecimal;

import org.bson.Document;
import org.bson.types.Decimal128;

import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.result.UpdateResult;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Updates.*;

public class Payment extends Transaction {
	
	public Payment(String database, MongoClient client, ReadConcern readConcern, WriteConcern writeConcern) {
		super(database, client, readConcern, writeConcern);
	}

	@Override
	public void process(String[] args) {
		System.out.println("-------- Payment --------");
		int w_id = Integer.parseInt(args[1]);
		int d_id = Integer.parseInt(args[2]);
		int c_id = Integer.parseInt(args[3]);
		BigDecimal payment = new BigDecimal(args[4]);

		updateWarehouse(w_id, payment);
		updateDistrict(w_id, d_id, payment);
		updateCustomer(w_id, d_id, c_id, payment);

		Document warehouse = getWarehouse(w_id);
		Document district = getDistrict(w_id, d_id);
		Document customer = getCustomer(w_id, d_id, c_id);
		// output
		System.out.printf(
				"C_IDENTIFIER: (%d, %d, %d), C_NAME: (%s, %s, %s), "
						+ "C_ADDRESS: (%s, %s, %s ,%s, %s), C_PHONE: %s, C_SINCE: %tc, "
						+ "C_CREDIT: %s,  C_CREDIT_LIM: %f, C_DISCOUNT: %f, C_BALANCE: %f\n",
				w_id, d_id, c_id, customer.getString("C_FIRST"), customer.getString("C_MIDDLE"),
				customer.getString("C_LAST"), customer.getString("C_STREET_1"), customer.getString("C_STREET_2"),
				customer.getString("C_CITY"), customer.getString("C_STATE"), customer.getString("C_ZIP"),
				customer.getString("C_PHONE"), customer.getDate("C_SINCE"), customer.getString("C_CREDIT"),
				customer.get("C_CREDIT_LIM", Decimal128.class).bigDecimalValue(), 
				customer.get("C_DISCOUNT", Decimal128.class).bigDecimalValue(), 
				customer.get("C_BALANCE", Decimal128.class).bigDecimalValue());
		System.out.printf("W_ADDRESS: (%s, %s, %s, %s, %s) \n", warehouse.getString("W_STREET_1"),
				warehouse.getString("W_STREET_2"), warehouse.getString("W_CITY"), warehouse.getString("W_STATE"),
				warehouse.getString("W_ZIP"));
		System.out.printf("D_ADDRESS: (%s, %s, %s, %s, %s) \n", district.getString("D_STREET_1"),
				district.getString("D_STREET_2"), district.getString("D_CITY"), district.getString("D_STATE"),
				district.getString("D_ZIP"));
	}

	private UpdateResult updateWarehouse(int w_id, BigDecimal payment) {
		UpdateResult result = getCollection("warehouse").updateOne(
				eq("W_ID", w_id),
				inc("W_YTD", payment));
		return result;
	}

	private UpdateResult updateDistrict(int w_id, int d_id, BigDecimal payment) {
		UpdateResult result = getCollection("district").updateOne(
				and(eq("D_W_ID", w_id), eq("D_ID", d_id)),
				inc("D_YTD", payment));
		return result;
	}

	private UpdateResult updateCustomer(int w_id, int d_id, int c_id, BigDecimal payment) {
		UpdateResult result = getCollection("customer").updateOne(
				and(eq("C_W_ID", w_id), eq("C_D_ID", d_id), eq("C_ID", c_id)),
				combine(inc("C_BALANCE", payment.negate()), 
						inc("C_YTD_PAYMENT", payment.doubleValue()), 
						inc("C_PAYMENT_CNT", 1)));
		return result;
	}
	
	private Document getWarehouse(int w_id) {
		Document doc = getCollection("warehouse")
				.find(eq("W_ID", w_id))
				.projection(fields(include("W_STREET_1"), include("W_STREET_2"), 
								   include("W_CITY"), include("W_STATE"),
								   include("W_ZIP"), excludeId()))
				.first();
		if (doc == null) {
			throw new IllegalArgumentException("No matching warehouse");
		}
		return doc;
	}

	private Document getDistrict(int w_id, int d_id) {
		Document doc = getCollection("district")
				.find(and(eq("D_W_ID", w_id), eq("D_ID", d_id)))
				.projection(fields(include("D_STREET_1"), include("D_STREET_2"), 
								   include("D_CITY"), include("D_STATE"),
								   include("D_ZIP"), excludeId()))
				.first();
		if (doc == null) {
			throw new IllegalArgumentException("No matching district");
		}
		return doc;
	}

	private Document getCustomer(int w_id, int d_id, int c_id) {
		Document doc = getCollection("customer")
				.find(and(eq("C_W_ID", w_id), eq("C_D_ID", d_id), eq("C_ID", c_id)))
				.projection(fields(include("C_STREET_1"), include("C_STREET_2"), 
								   include("C_CITY"), include("C_STATE"),
								   include("C_ZIP"), include("C_FIRST"),
								   include("C_MIDDLE"), include("C_LAST"),
								   include("C_PHONE"), include("C_SINCE"),
								   include("C_CREDIT"), include("C_CREDIT_LIM"),
								   include("C_DISCOUNT"), include("C_BALANCE"),
								   excludeId()))
				.first();
		if (doc == null) {
			throw new IllegalArgumentException("No matching customer");
		}
		return doc;
	}
}
