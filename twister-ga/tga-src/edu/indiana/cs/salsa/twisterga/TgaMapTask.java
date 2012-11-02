package edu.indiana.cs.salsa.twisterga;

import java.util.Random;
import java.util.Vector;

import cgl.imr.base.Key;
import cgl.imr.base.MapOutputCollector;
import cgl.imr.base.MapTask;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.MapperConf;
import cgl.imr.types.StringKey;

public class TgaMapTask implements MapTask {
	private int numReduceTasks = 0;
	
	public void close() throws TwisterException {
		//TODO Auto-generated method stub
	}
	
	//currently, Tga no static data to load
	public void configure(JobConf jobConf, MapperConf mapConf) throws TwisterException {
		this.numReduceTasks = jobConf.getNumReduceTasks();
		//TODO Figure out what kind of static data I can put here
		/*
		this.jobConf = jobConf;
		FileData fileData = (FileData) mapConf.getDataPartition();
		try {
			System.out.println(fileData.getFileName());
		} catch (Exception e) {
			throw new TwisterException(e);
		}
		*/
	}
	
	/*
	 * calculate the fitness value of each gene and emit genes to reducers
	 * the emit key-value pair is <geneId, gene>
	 */
	
	public void map(MapOutputCollector collector, Key key, Value val) throws TwisterException {
		
		System.out.println("Tga Map starts here");
		
		Population subPop = (Population)val;
		
		Vector<Gene> vecGene = subPop.getGenePool();
		System.out.println("Mapper: " + this.toString() + "Gene vec size: " + vecGene.size());
		
		Random rand = new Random();
		
		for (int i = 0; i < vecGene.size(); i++) {
			Gene gene = vecGene.get(i);
			gene.setGeneId(rand.nextInt());
			gene.calcFitValue();
		}
		
		int keyNo = 0;
		for (Gene gene : subPop.getGenePool()) {
			//System.out.println("collect gene: " + gene.getGeneId());
			//collector.collect(new StringKey("" + gene.getGeneId()), gene);
			//key should be a integer mod number of reducers so the workload can be distributed uniformly
			System.out.println("geneId " + gene.getGeneId());
			collector.collect(new StringKey("" + (keyNo++ % this.numReduceTasks)), gene);
		}
		
		System.out.println("Tga Map ends here");
		/*
		 * if the converge is too slow, we can track the best gene here like UIUC did
		 */
		
		
		
		//MemCacheAddress memCacheKey = (MemCacheAddress) val;
		//int index = memCacheKey.getStart();
		//int range = memCacheKey.getRange();
		
		//System.out.println(val.toString());
		//IntValue val1 = (IntValue)val;
		//System.out.println(val1.getVal());
		//IntValue val2 = (IntValue)val;
		//System.out.println(val2.getVal());
		
		
		//System.out.println(index);
		//System.out.println(range);
	}

}
