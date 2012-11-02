package edu.indiana.cs.salsa.twisterga;

import java.util.List;

import cgl.imr.base.Key;
import cgl.imr.base.ReduceOutputCollector;
import cgl.imr.base.ReduceTask;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.ReducerConf;
import cgl.imr.types.StringKey;

public class TgaReduceTask implements ReduceTask {
	private static final String GENELEN = "geneLen";
	private static final String TOURNAMENTSIZE = "tournamentSize";
	private static final String CROSSOVERWINDOW = "crossoverWindow";
	private static final String GENCOUNT = "genCount";
	private static final String EXCHANGERATE = "exchangeRate";
	
	private static int popSize;
	private static int geneLen;
	private static int tournamentSize;
	private static int crossoverWindow;
	private static int genCount;
	private static double exchangeRate;
	
	@Override
	public void close() throws TwisterException {
		// TODO Auto-generated method stub
	}
	
	@Override
	public void configure(JobConf jobConf, ReducerConf reducerConf)
			throws TwisterException {
		System.out.println("Start reduce config");
		geneLen = Integer.parseInt(jobConf.getProperty(GENELEN));
		tournamentSize = Integer.parseInt(jobConf.getProperty(TOURNAMENTSIZE));
		crossoverWindow = Integer.parseInt(jobConf.getProperty(CROSSOVERWINDOW));
		//genCount = Integer.parseInt(jobConf.getProperty(GENCOUNT));
		genCount = 1;
		exchangeRate = Double.parseDouble(jobConf.getProperty(EXCHANGERATE));
	}
	
	@Override
	/*
	 * collect genes to form a new population and exert select/crossover on it
	 * to form better population
	 */
	public void reduce(ReduceOutputCollector collector, Key key, 
			List<Value> values) throws TwisterException {
		System.out.println("Start reduce: " + this.toString());
		
		if (values.size() <= 0) {
			System.out.println("REDUCE INPUT ERROR");
			throw new TwisterException("Reduce input error no values");
		}
		
		popSize = values.size();
		System.out.println("reduce: " + this.toString() + " values size is " + popSize);
		
		Population subPop = new Population(popSize, tournamentSize, crossoverWindow, genCount, exchangeRate, geneLen, true);
		
		for (Value value : values) {
			//System.out.println(((Gene)value).getKey().toString());
			subPop.addGene((Gene)value);
		}
		
		
		subPop.startEvolve();
		
		//not sure if same key is allowed for combiner
		collector.collect(new StringKey("" + subPop.getCurrentGeneration()), subPop);
		System.out.println("End reduce");
	}
	
}
