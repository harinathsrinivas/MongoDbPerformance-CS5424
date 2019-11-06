package assign2;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class Experiment {
	private static final String SYSOUT_FOLDER = "xact-output";
	private static final String REPORT_FOLDER = "report";
	private static final String THROUGHPUT_REPORT_FILE = "throughput-report.txt";
	private static final String STATE_REPORT_FILE = "state-report.txt";
	private static final String[] CONNECTIONS = {
			"mongodb://192.168.56.159", 
			"mongodb://192.168.56.160",
			"mongodb://192.168.56.161",
			"mongodb://192.168.56.162",
			"mongodb://192.168.56.163"};
	
	public static void main(String[] args) {
		int nClients;
		boolean test;
		try {
			nClients = Integer.parseInt(args[0]);
			test = false;
		} catch (Exception e) {
			nClients = 1;
			test = true;
		}
		String readConcern = args[1];
		String writeConcern = args[2];
		boolean reloadData = args.length == 3;
		String xactFileDir = test ? "test-file" : "project-files/xact-files";
		String outputDir = String.format("experiment__nc_%d__r_%s__w_%s", 
				nClients, readConcern, writeConcern);
		Experiment exp = new Experiment(xactFileDir, outputDir, nClients, readConcern, writeConcern, reloadData);
		exp.run();
	}
	
	private String xactFileDir;
	private String outputDir;
	private String sysoutDir;
	private String reportDir;
	private int nClients;
	private String readConcern;
	private String writeConcern;
	boolean reloadData;
	
	public Experiment(String xactFileDir, String outputDir, 
			int nClients, String readConcern, String writeConcern,
			boolean reloadData) {
		this.xactFileDir = xactFileDir;
		this.outputDir = outputDir;
		this.sysoutDir = outputDir + System.getProperty("file.separator") + SYSOUT_FOLDER;
		this.reportDir = outputDir + System.getProperty("file.separator") + REPORT_FOLDER;
		this.nClients = nClients;
		this.readConcern = readConcern;
		this.writeConcern = writeConcern;
		this.reloadData = reloadData;
	}

	public void run() {
		makeFolders();
		if (reloadData) {
			LoadDataThread loadData = new LoadDataThread();
			loadData.start();
			try {
				loadData.join();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
				System.out.println("LoadData failed.");
				System.exit(0);
			}
		}
		List<Thread> threads = new ArrayList<Thread>();
		List<String> reportPaths = new ArrayList<String>();
		for (int i = 0; i < nClients; i++) {
			String connectionString = CONNECTIONS[i % CONNECTIONS.length];
			String xactFilepath = xactFileDir + System.getProperty("file.separator") + Integer.toString(i+1) + ".txt";
			String outputPath = sysoutDir + System.getProperty("file.separator") + Integer.toString(i+1) + "-output.txt";
			String reportPath = reportDir + System.getProperty("file.separator") + Integer.toString(i+1) + "-report.txt";
			FileDriverThread fileDriver = new FileDriverThread(
					connectionString, readConcern, writeConcern, xactFilepath, outputPath, reportPath);
			fileDriver.start();
			threads.add(fileDriver);
			reportPaths.add(reportPath);
		}
		for (Thread thread : threads) {
		    try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.out.println("Thread interrupted.");
				System.exit(0);
			}
		}
		String statePath = reportDir + System.getProperty("file.separator") + STATE_REPORT_FILE;
		DBStateThread dbState = new DBStateThread(CONNECTIONS[0], statePath);
		dbState.run();
		try {
			dbState.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.out.println("DBState failed.");
			System.exit(0);
		}
	}
	
	private void makeFolders() {
		File theDir = new File(outputDir);
		if (!theDir.exists()) {
			try {
				theDir.mkdir();
			} catch (SecurityException e) {
				e.printStackTrace();
				System.out.println("Unexpected error.");
				System.exit(0);
			}
		}
		File output = new File(sysoutDir);
		if (!output.exists()) {
			try {
				output.mkdir();
			} catch (SecurityException e) {
				e.printStackTrace();
				System.out.println("Unexpected error.");
				System.exit(0);
			}
		}
		File report = new File(reportDir);
		if (!report.exists()) {
			try {
				report.mkdir();
			} catch (SecurityException e) {
				e.printStackTrace();
				System.out.println("Unexpected error.");
				System.exit(0);
			}
		}
	}
}

class FileDriverThread extends Thread {

	private String connectionString;
	private String readConcern;
	private String writeConcern;
	private String xactFilepath;
	private String stdoutPath;
	private String stderrPath;
		
	public FileDriverThread(String connectionString, String readConcern, String writeConcern,
							String xactFilepath, String stdoutPath, String stderrPath) {
		this.connectionString = connectionString; 
		this.readConcern = readConcern;
		this.writeConcern = writeConcern;
		this.xactFilepath = xactFilepath;
		this.stdoutPath = stdoutPath;
		this.stderrPath = stderrPath;
	}

	public void run() {
		try {
	        String javaHome = System.getProperty("java.home");
	        String javaBin = javaHome +
	                File.separator + "bin" +
	                File.separator + "java";
	        String classpath = System.getProperty("java.class.path");
	        String className = FileDriver.class.getName();
			ProcessBuilder pb = new ProcessBuilder(javaBin, "-cp", classpath, className, 
					connectionString, readConcern, writeConcern, xactFilepath);
			pb.redirectOutput(new File(stdoutPath));
			pb.redirectError(new File(stderrPath));
			System.out.println("Processing: " + xactFilepath);
			Process extprocess = pb.start();
			extprocess.waitFor();
			System.out.println("Finished processing: " + xactFilepath);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Unable to start FileDriver");
			System.exit(0);
		}
	}
}

class LoadDataThread extends Thread {
		
	public LoadDataThread() {}

	public void run() {
		try {
	        String javaHome = System.getProperty("java.home");
	        String javaBin = javaHome +
	                File.separator + "bin" +
	                File.separator + "java";
	        String classpath = System.getProperty("java.class.path");
	        String className = LoadData.class.getName();
			ProcessBuilder pb = new ProcessBuilder(javaBin, "-cp", classpath, className);
			System.out.println("Loading Data...");
			Process extprocess = pb.start();
			BufferedReader stderr = new BufferedReader(new InputStreamReader(extprocess.getInputStream())); 
			String s = ""; 
			while ((s = stderr.readLine()) != null) { 
			     System.out.println(s); 
			} 
			extprocess.waitFor();
			System.out.println("Finished Loading");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Unable to start LoadData");
			System.exit(0);
		}
	}
}

class DBStateThread extends Thread {

	private String connectionString;
	private String stdoutPath;
		
	public DBStateThread(String connectionString, String stdoutPath) {
		this.connectionString = connectionString;
		this.stdoutPath = stdoutPath;
	}

	public void run() {
		try {
	        String javaHome = System.getProperty("java.home");
	        String javaBin = javaHome +
	                File.separator + "bin" +
	                File.separator + "java";
	        String classpath = System.getProperty("java.class.path");
	        String className = DBState.class.getName();
			ProcessBuilder pb = new ProcessBuilder(javaBin, "-cp", classpath, className, connectionString);
			pb.redirectOutput(new File(stdoutPath));
			System.out.println("Getting DatabaseState...");
			Process extprocess = pb.start();
			extprocess.waitFor();
			System.out.println("Finished");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Unable to start DBState");
			System.exit(0);
		}
	}
}