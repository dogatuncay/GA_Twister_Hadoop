package edu.indiana.cs.salsa.twisterga;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.Value;

public class Population implements Value {
	public int getPopSize() {
		return popSize;
	}

	public void setPopSize(int popSize) {
		this.popSize = popSize;
	}

	public int getTournamentSize() {
		return tournamentSize;
	}

	public void setTournamentSize(int tournamentSize) {
		this.tournamentSize = tournamentSize;
	}

	public int getCrossoverWindow() {
		return crossoverWindow;
	}

	public void setCrossoverWindow(int crossoverWindow) {
		this.crossoverWindow = crossoverWindow;
	}

	public int getGenerationCount() {
		return generationCount;
	}

	public void setGenerationCount(int generationCount) {
		this.generationCount = generationCount;
	}

	public double getExchangeRate() {
		return exchangeRate;
	}

	public void setExchangeRate(double exchangeRate) {
		this.exchangeRate = exchangeRate;
	}

	public int getCrossoverTime() {
		return crossoverTime;
	}

	public void setCrossoverTime(int crossoverTime) {
		this.crossoverTime = crossoverTime;
	}

	public int getSelectTime() {
		return selectTime;
	}

	public void setSelectTime(int selectTime) {
		this.selectTime = selectTime;
	}

	public double getAvgFitValue() {
		return avgFitValue;
	}

	public void setAvgFitValue(double avgFitValue) {
		this.avgFitValue = avgFitValue;
	}

	public int getProblemSize() {
		return problemSize;
	}

	public void setProblemSize(int problemSize) {
		this.problemSize = problemSize;
	}

	public void setGenePool(Vector<Gene> genePool) {
		this.genePool = genePool;
	}

	private int popSize;
	private int tournamentSize;
	private int crossoverWindow;
	private int generationCount;
	private int currentGeneration; //dynamic attr
	private double exchangeRate;
	private int crossoverTime;
	private int selectTime;
	private double avgFitValue; //dynamic attr
	private int problemSize;
	private Vector<Gene> genePool;
	private PrintWriter outFile;
	
	public int getCurrentGeneration() {
		return currentGeneration;
	}

	public void setCurrentGeneration(int currentGeneration) {
		this.currentGeneration = currentGeneration;
	}

	public Population(int _popSize, int _tournamentSize, int _crossoverWindow,
			int _generationCount, double _exchangeRate, int _problemSize, boolean isSubPop) {
		popSize = _popSize;
		tournamentSize = _tournamentSize;
		crossoverWindow = _crossoverWindow;
		generationCount = _generationCount;
		exchangeRate = _exchangeRate;
		problemSize = _problemSize;
		currentGeneration = 0;
		crossoverTime = 0;
		selectTime = 0;
		avgFitValue = 0.0;
		/*
		 * try { outFile = new PrintWriter(new FileWriter("record.txt")); }
		 * catch (IOException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); }
		 */
		if (! isSubPop)
			initGenePool();
		else
		{
			genePool = new Vector<Gene>();
			popSize = genePool.size();
		}
	}

	public Population(GaConf conf) {
	}

	public Population() {
		popSize = 0;
		tournamentSize = 0;
		crossoverWindow = 0;
		generationCount = 0;
		exchangeRate = 0;
		problemSize = 0;
		currentGeneration = 0;
		crossoverTime = 0;
		selectTime = 0;
		avgFitValue = 0.0;
		genePool = new Vector<Gene>();
	}

	public void loadGeneFromFile(final String fileName) {
		// read the initial gene info from a file

		/*
		 * try { DataInputStream inputStream = new DataInputStream(new
		 * BufferedInputStream(new FileInputStream(fileName))); } catch
		 * (FileNotFoundException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); }
		 * 
		 * for (int i = 0; i < popSize; i++) {
		 * 
		 * for (int j = ) }
		 */

	}

	public void startEvolve() {
		// outFile.println("initial pop is ");
		for (int i = 0; i < genePool.size(); i++) {
			// outFile.println(genePool.get(i).getKey() + " " +
			// genePool.get(i).getFitValue());
			// outFile.println("initial average fitness value is " +
			// calcAvgFitValue());
		}

		while (currentGeneration != generationCount) {
			Vector<Gene> offspringPool = new Vector<Gene>(popSize);
			
			System.out.println("");
			System.out.println("Generation(size " + genePool.size() + " ): "
					+ currentGeneration + " average fitness value: "
					+ calcAvgFitValue());
					
			// outFile.println();
			// how many rounds GA needs to conduct crossover operation
			int round = popSize / crossoverWindow;
			for (int i = 0; i < round; i++) {
				Vector<Gene> crossoverPool = new Vector<Gene>(crossoverWindow);
				for (int j = 0; j < crossoverWindow; j++) {
					crossoverPool.add(select());
				}
				crossover(crossoverPool, offspringPool);
			}
			genePool.clear();
			genePool.addAll(offspringPool);
			currentGeneration++;
		}

		/*
		System.out.println("final generation is");
		for (int i = 0; i < genePool.size(); i++) {
			System.out.println(genePool.get(i).getKey() + " "
					+ genePool.get(i).getFitValue());
		}
		*/

	}

	public void testEvolve() {
		Gene geneOne = select();
		Gene geneTwo = select();
		Gene geneChildOne = new Gene(0);
		Gene geneChildTwo = new Gene(0);

		outFile.println(geneOne.getKey().toString());
		outFile.println(geneTwo.getKey().toString());

		crossUtil(geneOne, geneTwo, geneChildOne, geneChildTwo);

		outFile.println(geneChildOne.getKey().toString());
		outFile.println(geneChildTwo.getKey().toString());
	}

	private void initGenePool() {
		genePool = new Vector<Gene>(popSize);
		// Random generator = new Random();
		for (int i = 0; i < popSize; i++) {
			Gene gene = new Gene(problemSize);
			genePool.add(gene);
		}
	}

	/*
	 * tournament selection without replacement conduct crossover operation when
	 * the crossvoer window is full
	 */

	private Gene select() {
		// outFile.println("select time is " + selectTime);
		selectTime++;
		Vector<Gene> vecGeneSelected = new Vector<Gene>(tournamentSize);
		Random randomGen = new Random();
		for (int i = 0; i < tournamentSize; i++) {
			// outFile.println(genePool.size());
			int indexToAdd = randomGen.nextInt(genePool.size());
			Gene geneSelected = genePool.get(indexToAdd);
			vecGeneSelected.add(geneSelected);
			// keep the without replacement semantics
			genePool.remove(geneSelected);
		}

		// restore the genePool as it is before the selection
		genePool.addAll(vecGeneSelected);
		Gene geneDominant = vecGeneSelected.get(0);
		for (int i = 1; i < tournamentSize; i++) {
			if (vecGeneSelected.get(i).getFitValue() > geneDominant
					.getFitValue()) {
				geneDominant = vecGeneSelected.get(i);
			}
		}
		// genePool.remove(geneDominant);

		// outFile.println("Gene " + geneDominant.getKey() + " " +
		// geneDominant.getFitValue() + " get picked in " +
		// "generation " + currentGeneration);

		return geneDominant;
	}

	private void crossover(Vector<Gene> crossPool, Vector<Gene> offspringPool) {

		double localAvgFit = 0.0, sum = 0.0;
		for (int i = 0; i < crossoverWindow; i++) {
			sum += crossPool.get(i).getFitValue();
		}
		localAvgFit = sum / (double) crossoverWindow;

		// outFile.println("mate pool average fitness value: " + localAvgFit);

		Gene parentMale = new Gene(0);
		Gene parentFemale = new Gene(0);
		Gene childMale = new Gene(0);
		Gene childFemale = new Gene(0);

		for (int i = 0; i < crossoverWindow; i += 2) {
			parentMale = crossPool.get(i);
			parentFemale = crossPool.get(i + 1);
			// outFile.println("Mate male: " + parentMale.getKey() + " " +
			// parentMale.getFitValue());
			// outFile.println("Mate female: " + parentFemale.getKey() + " " +
			// parentFemale.getFitValue());
			crossUtil(parentMale, parentFemale, childMale, childFemale);

			// emit new offspring into population to keep its size constant
			// outFile.println("add two new offspring");
			offspringPool.add(new Gene(childMale.getKey()));
			offspringPool.add(new Gene(childFemale.getKey()));
		}

	}

	private void crossUtil(Gene parentMale, Gene parentFemale, Gene childMale,
			Gene childFemale) {
		BitSet bsMale = parentMale.getKey();
		BitSet bsFemale = parentFemale.getKey();
		BitSet bsChildMale = new BitSet(Long.SIZE);
		BitSet bsChildFemale = new BitSet(Long.SIZE);

		Random randGen = new Random();
		for (int i = 0; i < bsMale.size(); i++) {
			if (randGen.nextDouble() < exchangeRate) {
				if (bsMale.get(i)) {
					bsChildMale.set(i);
				} else {
					bsChildMale.clear(i);
				}

				if (bsFemale.get(i)) {
					bsChildFemale.set(i);
				} else {
					bsChildFemale.clear(i);
				}
			} else {
				if (bsFemale.get(i)) {
					bsChildMale.set(i);
				} else {
					bsChildMale.clear(i);
				}

				if (bsMale.get(i)) {
					bsChildFemale.set(i);
				} else {
					bsChildFemale.clear(i);
				}
			}
		}
		childMale.setKey(bsChildMale);
		childFemale.setKey(bsChildFemale);
	}

	public double calcAvgFitValue() {
		long sum = 0;
		//System.out.println("the population size is " + popSize);
		for (int i = 0; i < popSize; i++) {
			if (genePool.isEmpty()) {
				System.out.println("Gene pool is empty");
			}
			genePool.get(i).calcFitValue();
			//System.out.println("fit value: " + genePool.get(i).getFitValue());
			sum += genePool.get(i).getFitValue();
		}
		return (double) sum / (double) popSize;
	}

	@Override
	/**
	 * load object data from bytes
	 */
	public void fromTwisterMessage(TwisterMessage message)
			throws SerializationException {
		//System.out.println("population message begin");
		this.popSize = message.readInt();
		//System.out.println("pop size " + popSize);
		this.avgFitValue = message.readDouble();
		//System.out.println("avaFitValue" + avgFitValue);
		for (int i = 0; i < popSize; i++) {
			this.genePool.add(new Gene(message));
		}
		//System.out.println("gene pool size " + genePool.size());
	}

	@Override
	/**
	 * transfer object data to bytes
	 */
	public void toTwisterMessage(TwisterMessage message)
			throws SerializationException {
		message.writeInt(this.popSize);
		message.writeDouble(this.avgFitValue);
		for (Gene gene: genePool) {
			gene.toTwisterMessage(message);
		}
	}

	public void addGene(Gene gene) {
		this.genePool.add(gene);
		this.popSize++;
	}

	public double getFitnessVal() {
		return this.avgFitValue;
	}

	/*
	 * divide the population into several subPops so that each mapper can work
	 * on its own pop part
	 */
	public List<Population> getSubPops(int numMappers) {
		// int subPopSize = this.popSize / numMappers;

		ArrayList<Population> subPops = new ArrayList<Population>();
		for (int i = 0; i < numMappers; i++) {
			Population pop = new Population();
			pop.crossoverTime = this.crossoverTime;
			pop.crossoverWindow = this.crossoverWindow;
			pop.exchangeRate = this.exchangeRate;
			pop.generationCount = this.generationCount;
			pop.problemSize = this.problemSize;
			subPops.add(pop);
		}

		for (int j = 0; j < this.popSize; j++) {
			Population pop = subPops.get(j % numMappers);
			pop.addGene(this.genePool.get(j));
		}

		for (int k = 0; k < numMappers; k++) {
			subPops.get(k).setAvgFitValue(subPops.get(k).calcAvgFitValue());
			System.out.println("sub pop size is " + subPops.get(k).getPopSize());
			System.out.println("sub pop " + k + " fitness value is "
					+ subPops.get(k).getFitnessVal());
		}

		return subPops;
	}
	
	public Vector<Gene> getGenePool() {
		return this.genePool;
	}
}
