package assign2;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.math.Quantiles;
import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class FileDriver {
	private static final String DATABASE = LoadData.DATABASE;

	public static void main(String[] args) {
		LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
		Logger rootLogger = loggerContext.getLogger("org.mongodb.driver");
		rootLogger.setLevel(Level.OFF);
		
		String connectionString = args[0];
		ReadConcern readConcern = toReadConcern(args[1]);
		WriteConcern writeConcern = toWriteConcern(args[2]);
		String xactFilepath = args[3];
		FileDriver driver = new FileDriver(connectionString, readConcern, writeConcern);
		driver.process(xactFilepath);
	}
	
	private static ReadConcern toReadConcern(String string) {
		switch (string) {
		case "majority":
			return ReadConcern.MAJORITY;
		case "local":
			return ReadConcern.LOCAL;
		case "1":
			return ReadConcern.LOCAL;
		case "R1":
			return ReadConcern.LOCAL;
		default:
			throw new Error("Invalid ReadConcern");
		}
	}
	
	private static WriteConcern toWriteConcern(String string) {
		switch (string) {
		case "majority":
			return WriteConcern.MAJORITY;
		case "3":
			return WriteConcern.W3;
		case "W3":
			return WriteConcern.W3;
		case "1":
			return WriteConcern.W1;
		case "W1":
			return WriteConcern.W1;
		default:
			throw new Error("Invalid ReadConcern");
		}
	}

	private String connectionString;
	private ReadConcern readConcern;
	private WriteConcern writeConcern;
		
	public FileDriver(String connectionString, ReadConcern readConcern, WriteConcern writeConcern) {
		this.connectionString = connectionString; 
		this.readConcern = readConcern;
		this.writeConcern = writeConcern;
	}
	
	public void process(String xactFilepath) {
		List<Long> transactionTimes;
		try {
			Scanner sc = new Scanner(new File(xactFilepath));
			transactionTimes = processTransactions(sc);
			sc.close();
		} catch (FileNotFoundException e) {
			throw new Error("File not found: " + xactFilepath);
		}
		outputTransactionStats(transactionTimes);
	}

	private List<Long> processTransactions(Scanner sc) {
		MongoClient client = MongoClients.create(connectionString);
        TransactionFactory factory = new TransactionFactory(DATABASE, client, readConcern, writeConcern);
        List<Long> transactionTimes = new ArrayList<Long>();
        while (sc.hasNext()) {
        	long startTime = System.currentTimeMillis();
            String[] args = sc.nextLine().split(",");
            String type = args[0];
            if (type.equals("N")) {
                int n = Integer.parseInt(args[args.length - 1]);
                List<String> orderLines = new ArrayList<String>();
                for (int i = 0; i < n; i++) {
                    orderLines.add(sc.nextLine());
                }
                String csv = String.join("\n", orderLines);
                
                List<String> argsList = new ArrayList<String>();
                for (String arg: args) {
                	argsList.add(arg);
                }
                argsList.add(csv);
                args = argsList.toArray(new String[argsList.size()]);
            }
            Transaction transaction = factory.getTransaction(type);
            transaction.process(args);
            long endTime = System.currentTimeMillis();
            transactionTimes.add(endTime - startTime);
        }
        client.close();
		return transactionTimes;
    }
	
	private void outputTransactionStats(List<Long> transactionTimes) {
		int n = transactionTimes.size();
		long total = transactionTimes.stream().mapToLong(Long::longValue).sum();
		double totalSec = (double) total / 100.0;
		double throughput = (double) n / totalSec;
        double average = (double) total / (double) n;
		double median = Quantiles.median().compute(transactionTimes);
		double percentile95 = Quantiles.percentiles().index(95).compute(transactionTimes);
		double percentile99 = Quantiles.percentiles().index(99).compute(transactionTimes);
		System.err.println(String.format("Number of Transactions: %d", n));
		System.err.println(String.format("Total Transcation Time: %f sec", totalSec));
		System.err.println(String.format("Throughput: %f txns/sec", throughput));
		System.err.println(String.format("Average Transcation Latency: %f ms", average));
		System.err.println(String.format("Median Transcation Latency: %f ms", median));
		System.err.println(String.format("95 Percentile Transcation Latency: %f ms", percentile95));
		System.err.println(String.format("99 Percentile Transcation Latency: %f ms", percentile99));
	}
}
