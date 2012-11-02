package edu.indiana.cs.salsa.twisterga;

public class Onemax {

	public static void main(String [] args) {
		Onemax.run();
	}
	
	private static void run() {
		GaConf conf = new GaConf("bioInfo.xml");
		
		Population pop = new Population(conf.getPopSize(), conf.getTournamentSize(), 
				conf.getCrossoverWindow(), conf.getGenerationCount(), conf.getExchangeRate(), conf.getProblemSize(), false);
		
		pop.startEvolve();
		
	}
}
