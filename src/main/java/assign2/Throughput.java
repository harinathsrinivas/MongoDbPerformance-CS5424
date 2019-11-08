package assign2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

public class Throughput {

	public static void main(String[] args) {
		int nc = Integer.parseInt(args[0]);
		String reportDir = args[1];
		
		String throughputPath = reportDir + System.getProperty("file.separator") + THROUGHPUT_REPORT_FILE;
		List<String> reportPaths = getReportPaths(reportDir, nc);
		List<Double> throughputs = new ArrayList<Double>();
		for (String path : reportPaths) {
			throughputs.add(getThroughput(path));
		}
		DoubleSummaryStatistics statistics = throughputs.stream().collect(Collectors.summarizingDouble(Double::doubleValue));
		System.out.println("Writing Throughput stats");
		try {
			FileWriter  fileWriter = new FileWriter(throughputPath);
		    PrintWriter printWriter = new PrintWriter(fileWriter);
		    printWriter.printf("Min Throughput: %f txns/sec %n", statistics.getMin());
		    printWriter.printf("Avg Throughput: %f txns/sec %n", statistics.getAverage());
		    printWriter.printf("Max Throughput: %f txns/sec %n", statistics.getMax());
		    printWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Write Throughput failed.");
			System.exit(0);
		}
	}
	
	private static final String THROUGHPUT_REPORT_FILE = "throughput-report.txt";

	private static List<String> getReportPaths(String reportDir, int nc) {
		List<String> reportPaths = new ArrayList<String>();
		for (int i = 1; i < nc+1; i++) {
			reportPaths.add(reportDir + System.getProperty("file.separator") + Integer.toString(i) + "-report.txt");
		}
		return reportPaths;
	}

	private static double getThroughput(String filepath) {
		try {
			Scanner sc = new Scanner(new File(filepath));
			double throughput = 0;
	        while (sc.hasNext()) {
	            String[] parts = sc.nextLine().split(" ");
	            if (parts[0].equals("Throughput:")) {
	            	throughput = Double.parseDouble(parts[1]);
	            	break;
	            }
	        }
			sc.close();
			if (throughput == 0) {
				throw new Error("No throughput in " + filepath);
			}
			return throughput;
		} catch (FileNotFoundException e) {
			throw new Error("File not found: " + filepath);
		}
	}
}
