package assign2;

import org.bson.Document;

import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;

public abstract class Transaction {
	private MongoClient client;
	private String database;
	private ReadConcern readConcern;
	private WriteConcern writeConcern;

	public Transaction(String database, MongoClient client, ReadConcern readConcern, WriteConcern writeConcern) {
		this.database = database;
		this.client = client;
		this.readConcern = readConcern;
		this.writeConcern = writeConcern;
	}
	
	protected MongoCollection<Document> getCollection(String collectionName) {
		return client.getDatabase(database).withReadConcern(readConcern)
										   .withWriteConcern(writeConcern)
										   .getCollection(collectionName);
	}

	public abstract void process(String[] args);
}

