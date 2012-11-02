package edu.indiana.cs.salsa.twisterga;

import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class ConfigXmlParser {
	
	public ConfigXmlParser(String filePath) {
		parseXmlFile(filePath);
		parseDom();
	}
	
	private int popSize;
	public int getPopSize() {
		return popSize;
	}

	public String getCrossoverType() {
		return crossoverType;
	}

	public double getExchangeRate() {
		return exchangeRate;
	}

	public double getCrossoverProb() {
		return crossoverProb;
	}

	public double getMutate() {
		return mutate;
	}
	
	public int getTournamentSize() {
		return tournamentSize;
	}
	
	public int getCrossoverWindow() {
		return crossoverWindow;
	}
	
	public int getGenerationCount() {
		return generationCount;
	}
	
	public int getProblemSize() {
		return problemSize;
	}


	private String crossoverType;
	private double exchangeRate;
	private double crossoverProb;
	private double mutate;
	private int    tournamentSize;
	private int    crossoverWindow;
	private int    generationCount;
	private int   problemSize;

	private Document dom;
	
	private void parseXmlFile(String filePath) {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		
		try {
			DocumentBuilder db = dbf.newDocumentBuilder();
			dom = db.parse(filePath);
		}catch(ParserConfigurationException pce) {
			pce.printStackTrace();
		}catch(SAXException se) {
			se.printStackTrace();
		}catch(IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	private void parseDom() {
		Element root = dom.getDocumentElement();
		
		NodeList popSizeList          = root.getElementsByTagName("populationSize");
		NodeList reproductionTypeList = root.getElementsByTagName("reproductionType");
		NodeList crossoverList        = root.getElementsByTagName("crossover");
		NodeList mutateList           = root.getElementsByTagName("mutationProb");
		NodeList tournamentSizeList   = root.getElementsByTagName("tournamentSize");
		NodeList crossoverWindowList  = root.getElementsByTagName("crossoverWindow");
		NodeList generationCountList  = root.getElementsByTagName("generationCount");
		NodeList problemSizeList      = root.getElementsByTagName("problemSize");
		
		//only one element in popSizeList so 
		popSize             = Integer.parseInt(popSizeList.item(0).getFirstChild().getNodeValue());
		crossoverType       = reproductionTypeList.item(0).getFirstChild().getNodeValue();
		mutate              = Double.parseDouble(mutateList.item(0).getFirstChild().getNodeValue());
		tournamentSize      = Integer.parseInt(tournamentSizeList.item(0).getFirstChild().getNodeValue());
		crossoverWindow     = Integer.parseInt(crossoverWindowList.item(0).getFirstChild().getNodeValue());
		generationCount     = Integer.parseInt(generationCountList.item(0).getFirstChild().getNodeValue());
		problemSize         = Integer.parseInt(problemSizeList.item(0).getFirstChild().getNodeValue());
		
		//replace the second parameter as the desired crossover type
		Element crossoverElem = getCrossoverElem(crossoverList, "Uniform");
		
		if (crossoverElem != null) {
			NodeList probNl = crossoverElem.getElementsByTagName("probability");
			NodeList exchangeNl = crossoverElem.getElementsByTagName("exchangeRate");
			
			crossoverProb = Double.parseDouble(probNl.item(0).getFirstChild().getNodeValue());
			exchangeRate  = Double.parseDouble(exchangeNl.item(0).getFirstChild().getNodeValue());
			
		}
		
	}
	
	private Element getCrossoverElem(NodeList nodeList, String type) {
		for (int i = 0; i < nodeList.getLength(); i++) {
			if (nodeList.item(i).getAttributes().getNamedItem("type").getNodeValue().equals(type)) {
				return (Element)nodeList.item(i);
			}
		}
		return null;
	}

}
