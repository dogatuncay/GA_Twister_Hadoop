package edu.indiana.cs.salsa.twisterga;

import java.util.Map;

import cgl.imr.base.Combiner;
import cgl.imr.base.Key;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;

public class TgaCombiner implements Combiner{
	private static final String POPSIZE = "popSize";
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
	
	private Population result;
	
	public TgaCombiner() {
		result = new Population();
	}
	
	@Override
	/*
	 * collect all population into one list
	 */
	public void combine(Map<Key, Value> keyValues) throws TwisterException {
		System.out.println("Start combiner");
		
		result.setPopSize(popSize);
		result.setCrossoverWindow(crossoverWindow);
		result.setGenerationCount(genCount);
		result.setExchangeRate(exchangeRate);
		result.setProblemSize(geneLen);
		result.setTournamentSize(tournamentSize);
		
		for (Value value : keyValues.values()) {
			result.getGenePool().addAll(((Population)value).getGenePool());
		}
		
		System.out.println("expected pop size is " + popSize);
		System.out.println("combined pop size is " + result.getGenePool().size());
		//result.setAvgFitValue(result.calcAvgFitValue());
		
	}
	
	@Override
	public void configure(JobConf jobConf) throws TwisterException{
		popSize = Integer.parseInt(jobConf.getProperty(POPSIZE));
		geneLen = Integer.parseInt(jobConf.getProperty(GENELEN));
		tournamentSize = Integer.parseInt(jobConf.getProperty(TOURNAMENTSIZE));
		crossoverWindow = Integer.parseInt(jobConf.getProperty(CROSSOVERWINDOW));
		genCount = Integer.parseInt(jobConf.getProperty(GENCOUNT));
		exchangeRate = Double.parseDouble(jobConf.getProperty(EXCHANGERATE));
	}
	
	public Population getResults() {
		return result;
	}
}
