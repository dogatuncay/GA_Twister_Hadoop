package edu.indiana.cs.salsa.twisterga;

import org.doomdark.uuid.UUIDGenerator;

import cgl.imr.base.KeyValuePair;
import cgl.imr.base.TwisterMonitor;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;


public class TgaMain {
	public static void main(String[] args) {
		if (args.length != 4) {
			String errReport = "Usage: java.edu.indiana.cs.salsa.twisterga.TgaMain <bioInfo file> <num map tasks> <num reduce tasks> <partition file>";
			System.out.println(errReport);
			System.exit(0);
		}
		
		int numMapTasks = Integer.parseInt(args[1]);
		int numReduceTasks = Integer.parseInt(args[2]);
		String partitionFile = args[3];
		
		TgaMain client;
		
		try {
			client = new TgaMain();
			double beginTime = System.currentTimeMillis();
			client.driveMapReduce(partitionFile, numMapTasks, numReduceTasks, args[0]);
			double endTime = System.currentTimeMillis();
			System.out.println("---------------------------------------------");
			System.out.println("TgaMain took " + (endTime - beginTime) / 1000 + "seconds.");
			System.out.println("---------------------------------------------");
		} catch(Exception e) {
			e.printStackTrace();
		}
		System.exit(0);
	}
	
	private UUIDGenerator uuidGen = UUIDGenerator.getInstance();
	
	public void driveMapReduce(String partitionFile, int numMapTasks, int numReduceTasks, String confFilePath) throws Exception {
		long beforeTime = System.currentTimeMillis();
		
		// JobConfigurations
		JobConf jobConf = new JobConf("Onemax-ga-map-reduce" + uuidGen.generateTimeBasedUUID());
		jobConf.setMapperClass(TgaMapTask.class);
		jobConf.setReducerClass(TgaReduceTask.class);
		//jobConf.setCombinerClass(TgaCombiner.class);
		jobConf.setNumMapTasks(numMapTasks);
		jobConf.setNumReduceTasks(numReduceTasks);
		
		TwisterDriver driver = new TwisterDriver(jobConf);
		driver.configureMaps(partitionFile);
		
		TwisterMonitor monitor = null;
		
		GaConf gaConf = new GaConf(confFilePath);
		int maxGen = gaConf.getGenerationCount();
		for (int gen = 0; gen < maxGen; gen++) {
			driver.runMapReduce();
			monitor.monitorTillCompletion();
		}
	}
	

}
