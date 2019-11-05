package assign2;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class TransactionFactory {
	
	public static void main(String[] args) {
		Logger.getLogger("org.mongodb.driver").setLevel(Level.SEVERE);

		MongoClient client = MongoClients.create();
		TransactionFactory factory = new TransactionFactory("wholesale", client, ReadConcern.LOCAL, WriteConcern.W1);
	
		Transaction t;
		String csv;
		String[] targs;
		
		String[] newOrderArgs = {
				"N", "1279", "1", "1" , "15",
			    "68195,1,1\n26567,1,5\n4114,1,7\n69343,1,3\n1836,1,1\n89294,1,3\n31975,1,6\n80487,1,10\n32423,1,5\n"
			     + "93383,1,9\n46279,1,9\n98511,1,8\n69351,1,4\n4679,1,6\n96983,1,2"
		};
		targs = newOrderArgs;
		t = factory.getTransaction(targs[0]);
		t.process(targs);
		
		csv = "P,1,1,1279,723.94";
		targs = csv.split(",");
		t = factory.getTransaction(targs[0]);
		t.process(targs);
		
		csv = "D,1,1";
		targs = csv.split(",");
		t = factory.getTransaction(targs[0]);
		t.process(targs);
		
		csv = "O,1,1,1279";
		targs = csv.split(",");
		t = factory.getTransaction(targs[0]);
		t.process(targs);
		
		csv = "S,1,1,12,34";
		targs = csv.split(",");
		t = factory.getTransaction(targs[0]);
		t.process(targs);
		
		csv = "I,1,1,5";
		targs = csv.split(",");
		t = factory.getTransaction(targs[0]);
		t.process(targs);
		
		csv = "T";
		targs = csv.split(",");
		t = factory.getTransaction(targs[0]);
		t.process(targs);
		
		csv = "R,1,1,1279";
		targs = csv.split(",");
		t = factory.getTransaction(targs[0]);
		t.process(targs);
	}
    
	private MongoClient client;
	private String database;
	private ReadConcern readConcern;
	private WriteConcern writeConcern;
	
	public TransactionFactory(String database, MongoClient client, ReadConcern readConcern, WriteConcern writeConcern) {
		this.database = database;
		this.client = client;
		this.readConcern = readConcern;
		this.writeConcern = writeConcern;
	}
	
	public Transaction getTransaction(String type) {
		switch (type) {
			case "N":
				return new NewOrder(database, client, readConcern, writeConcern);
			case "P":
				return new Payment(database, client, readConcern, writeConcern);
			case "D":
				return new Delivery(database, client, readConcern, writeConcern);
			case "O":
				return new OrderStatus(database, client, readConcern, writeConcern);
			case "S":
				return new StockLevel(database, client, readConcern, writeConcern);
			case "I":
				return new PopularItem(database, client, readConcern, writeConcern);
			case "T":
				return new TopBalance(database, client, readConcern, writeConcern);
			case "R":
				return new RelatedCustomer(database, client, readConcern, writeConcern);
			default:
				throw new IllegalArgumentException("invalid transaction type '" + type + "'");
		}
	}
}
