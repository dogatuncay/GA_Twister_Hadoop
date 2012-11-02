package edu.indiana.cs.salsa.twisterga;

public class GaConf {
	private ConfigXmlParser configXmlParser;
	
	public GaConf(String filePath) {
		configXmlParser = new ConfigXmlParser(filePath);
	}
	
	public int getPopSize() {
		return configXmlParser.getPopSize();
	}
	
	public double getCrossoverProb() {
		return configXmlParser.getCrossoverProb();
	}
	
	public double getExchangeRate() {
		return configXmlParser.getExchangeRate();
	}
	
	public double getMutateRate() {
		return configXmlParser.getMutate();
	}
	
	public int getCrossoverWindow() {
		return configXmlParser.getCrossoverWindow();
	}
	
	public int getTournamentSize() {
		return configXmlParser.getTournamentSize();
	}
	
	public int getGenerationCount() {
		return configXmlParser.getGenerationCount();
	}

	public int getProblemSize() {
		return configXmlParser.getProblemSize();
	}
}
