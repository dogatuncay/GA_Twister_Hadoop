package edu.indiana.cs.salsa.twisterga;

import java.util.ArrayList;
import java.util.List;

import org.doomdark.uuid.UUIDGenerator;

import cgl.imr.base.KeyValuePair;
import cgl.imr.base.TwisterMonitor;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;
import cgl.imr.types.IntKey;
import cgl.imr.types.StringValue;

/**
 * Generate data for a genetic algorithm - onemax - using MapReduce. It uses a "map-only"
 * operation to generate data concurrently.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com)
 * 
 */

public class TgaDataGen {

	public static String DATA_FILE_SUFFIX = ".txt";
	
	public static String PROP_NUM_GENE_PER_MAP = "genes_per_map";
	public static String GENE_LENGTH = "";
	
	/**
	 * Produces a list of key,value pairs for map tasks.
	 * 
	 * @param numMaps
	 *            - Number of map tasks.
	 * @return - List of key,value pairs.
	 */
	private static List<KeyValuePair> getKeyValuesForMap(int numMaps,
			String dataFilePrefix, String dataDir) {
		List<KeyValuePair> keyValues = new ArrayList<KeyValuePair>();
		IntKey key = null;
		StringValue value = null;
		System.out.println(numMaps);
		for (int i = 0; i < numMaps; i++) {
			key = new IntKey(i);
			value = new StringValue(dataDir + "/" + dataFilePrefix + i
					+ DATA_FILE_SUFFIX);
			keyValues.add(new KeyValuePair(key, value));
		}
		System.out.println(value);
		return keyValues;
	}
	
	public static void main(String[] args) {
	
		System.out.println("gene args.len:"+args.length);
		if (args.length != 4) {
			String errorReport = "TgaDataGen: The Correct arguments are \n"
					+ "java edu.indiana.cs.salsa.twisterga.TgaDataGen  [bioInfo file][sub dir][data file prefix][num splits=num maps]";
			System.out.println(errorReport);
			System.exit(0);
		}
		
		List<String> argsForMapper = new ArrayList<String>();
		GaConf gaConf = new GaConf(args[0]);
		long popSize = gaConf.getPopSize();
		long numOfTasks = Long.parseLong(args[3]);
		if (popSize % numOfTasks != 0) {
			System.err.println("Number of population size is not equally divisable to map tasks");
			System.exit(0);
		}
		
		argsForMapper.add(String.valueOf(popSize/numOfTasks));
		argsForMapper.add(args[1]);
		argsForMapper.add(args[2]);
		argsForMapper.add(args[3]);
		argsForMapper.add(String.valueOf(gaConf.getProblemSize()));

		System.out.println("pop size is " + popSize);
		
		
		
		
		TgaDataGen client;
		try {
			client = new TgaDataGen();
			client.driveMapReduce(argsForMapper);
		} catch (Exception e) {
			e.printStackTrace();
		}
		//getKeyValuesForMap(2, "prefix", "dataDir");
		System.exit(0);
	}
	
	private UUIDGenerator uuidGen = UUIDGenerator.getInstance();
	
	public void driveMapReduce(List<String> args) throws Exception {
		long numGenesPerMap = Long.parseLong(args.get(0));
		String dataDir = args.get(1);
		String dataFilePrefix = args.get(2);
		int numOfMapTasks = Integer.parseInt(args.get(3));
		
		int numOfReduceTasks = 0; // it's a map only program
		
		// JobConfigurations
		JobConf jobConf = new JobConf("genetic-onemax-data-gen" + uuidGen.generateTimeBasedUUID());
		jobConf.setMapperClass(TgaDataGenMapTask.class);
		jobConf.setNumMapTasks(numOfMapTasks);
		jobConf.setNumReduceTasks(numOfReduceTasks);
		jobConf.addProperty(PROP_NUM_GENE_PER_MAP, String.valueOf(numGenesPerMap));
		jobConf.addProperty(GENE_LENGTH, String.valueOf(args.get(4)));
		
		System.out.println("before driver");
		TwisterDriver driver = new TwisterDriver(jobConf);
		System.out.println("after driver");
		driver.configureMaps();
		System.out.println("after config map");
		TwisterMonitor monitor = driver.runMapReduce(getKeyValuesForMap(numOfMapTasks, dataFilePrefix, dataDir));
		monitor.monitorTillCompletion();
		driver.close();
	}
}
