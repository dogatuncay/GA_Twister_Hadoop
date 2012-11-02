package edu.indiana.cs.salsa.twisterga;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.safehaus.uuid.UUIDGenerator;

import cgl.imr.base.KeyValuePair;
import cgl.imr.base.TwisterModel;
import cgl.imr.base.TwisterMonitor;
import cgl.imr.base.impl.JobConf;
import cgl.imr.client.TwisterDriver;
import cgl.imr.types.StringKey;

/*
 * Tga: generate the initial population and store it in binary format on client in order to reduce network bandwidth and memory usage
 * All GA parameters are read from bioInof.xml
 */

public class Tga {
	private static int popSize;
	private static int geneLen;
	private static int tournamentSize;
	private static int crossoverWindow;
	private static int genCount;
	private static double exchangeRate;
	
	private static final String dataFile = "dataFile";
	private static final String GENELEN = "geneLen";
	private static final String TOURNAMENTSIZE = "tournamentSize";
	private static final String CROSSOVERWINDOW = "crossoverWindow";
	private static final String GENCOUNT = "genCount";
	private static final String EXCHANGERATE = "exchangeRate";
	private static final String POPSIZE = "popSize";

	private Population pop;
	
	public static void main(String[] args) {
		
		GaConf gaConf = new GaConf(args[0]);
		popSize = gaConf.getPopSize();
		geneLen = gaConf.getProblemSize();
		tournamentSize = gaConf.getTournamentSize();
		crossoverWindow = gaConf.getCrossoverWindow();
		genCount = gaConf.getGenerationCount();
		exchangeRate = gaConf.getExchangeRate();
		
		// calculate the number of "long" to write into the initial pop file 
		
		int numOfLongToWrite = geneLen / Long.SIZE;
		System.out.println("to write " + numOfLongToWrite + " longs to file");
		System.out.println("popSize is " + popSize + " geneLen is " + geneLen);
		
		Random rand = new Random();
		try {
			DataOutputStream initFile = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(dataFile)));
			for (int j = 0; j < popSize; j++) {
				for (int i = 0; i < numOfLongToWrite; i++) {
					initFile.writeLong(rand.nextLong());
				}
			}
			initFile.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		Population pop = new Population(popSize, tournamentSize, crossoverWindow, genCount, exchangeRate, geneLen, false);
		Tga tga = new Tga(pop);
		
		//distribute the initial gene data to all maps
		try {
			double beginTime = System.currentTimeMillis();
			tga.driveMapReduce(Integer.parseInt(args[1]), Integer.parseInt(args[2]));
			double endTime = System.currentTimeMillis();
			System.out.println("---------------------------------");
			System.out.println("tga took " + (endTime - beginTime) / 1000 + "seconds to converge.");
			System.out.println("---------------------------------");
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	public Tga(Population pop) {
		this.pop = pop;
	}
	
	private UUIDGenerator uuidGen = UUIDGenerator.getInstance();
	
	public void driveMapReduce(int numMapTasks, int numReduceTasks) throws Exception {
		long beginTime = System.currentTimeMillis();
		
		System.out.println("Num Maps: " + numMapTasks + " num Reducers: " + numReduceTasks);
		
		//JobConfig
		JobConf jobConf = new JobConf("Twister Genetic Algorithm Mapreduce" + uuidGen.generateTimeBasedUUID());
		jobConf.addProperty(Tga.POPSIZE, Integer.toString(popSize));
		jobConf.addProperty(Tga.CROSSOVERWINDOW, Integer.toString(Tga.crossoverWindow));
		jobConf.addProperty(Tga.GENCOUNT, Integer.toString(Tga.genCount));
		jobConf.addProperty(Tga.EXCHANGERATE, Double.toString(Tga.exchangeRate));
		jobConf.addProperty(Tga.TOURNAMENTSIZE, Integer.toString(Tga.tournamentSize));
		jobConf.addProperty(Tga.GENELEN, Integer.toString(Tga.geneLen));
		
		jobConf.setMapperClass(TgaMapTask.class);
		jobConf.setReducerClass(TgaReduceTask.class);
		jobConf.setCombinerClass(TgaCombiner.class);
		jobConf.setNumMapTasks(numMapTasks);
		jobConf.setNumReduceTasks(numReduceTasks);
		jobConf.setFaultTolerance();
		
		TwisterModel mrDriver = new TwisterDriver(jobConf);
		mrDriver.configureMaps();
		
		int curGen = 0;
		TwisterMonitor monitor = null;
		for (; curGen < genCount; curGen++) {
			//System.out.println("Pop contains genes: ");
			/*
			for (Gene gene : this.pop.getGenePool()) {
				System.out.println(gene.getKey().toString());
			}
			*/
			monitor = mrDriver.runMapReduce(getKeyValuesForMap(numMapTasks));
			monitor.monitorTillCompletion();
			
			Population newPop = ((TgaCombiner)mrDriver.getCurrentCombiner()).getResults();
			newPop.setCurrentGeneration(curGen);
			newPop.setAvgFitValue(newPop.calcAvgFitValue());
			
			this.pop = newPop;
			System.out.println("Generation: " + curGen);
			System.out.println("Gene pool size is " + this.pop.getPopSize());
			System.out.println("Avg fitness value is " + this.pop.getFitnessVal());
			System.out.println("");
		}
		
		double timeInSeconds = ((double)(System.currentTimeMillis() - beginTime)) / 1000;
		
		System.out.println("Total time for Tga : " + timeInSeconds);
		System.out.println("Total generation : " + genCount);
		
		mrDriver.close();
	}
	
	/*
	 * generate key value pair according to the number of map tasks, partition population into different subPops
	 * 
	 * @param numMappers
	 * @return
	 */
	private List<KeyValuePair> getKeyValuesForMap(int numMappers) {
		List<KeyValuePair> keyValues = new ArrayList<KeyValuePair>();
		List<Population> subPops = pop.getSubPops(numMappers);
		
		StringKey key = null;
		
		int keyNo = 0;
		for (Population pop : subPops) {
			key = new StringKey("" + keyNo);
			keyValues.add(new KeyValuePair(key, pop));
			keyNo++;			
			//System.out.println("sub pop" + keyNo + " size is " + pop.getGenePool().size() + " == " + pop.getPopSize());
		}
		
		//System.out.println("about to send key-value to mappers");
		return keyValues;
	}
}
