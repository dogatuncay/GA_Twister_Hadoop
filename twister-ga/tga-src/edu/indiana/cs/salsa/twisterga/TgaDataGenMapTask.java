package edu.indiana.cs.salsa.twisterga;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Random;

import cgl.imr.base.Key;
import cgl.imr.base.MapOutputCollector;
import cgl.imr.base.MapTask;
import cgl.imr.base.TwisterException;
import cgl.imr.base.Value;
import cgl.imr.base.impl.JobConf;
import cgl.imr.base.impl.MapperConf;
import cgl.imr.config.TwisterConfigurations;
import cgl.imr.types.StringValue;

public class TgaDataGenMapTask implements MapTask{
	private JobConf jobConf;
	
	@Override
	public void close() throws TwisterException {
		
	}
	
	public void configure(JobConf jobConf, MapperConf mapConf) throws TwisterException {
		this.jobConf = jobConf;
	}
	
	public void map(MapOutputCollector collector, Key key, Value val) throws TwisterException {
		long numGeneToGen = Long.parseLong(jobConf.getProperty(TgaDataGen.PROP_NUM_GENE_PER_MAP));
		long geneLength = Integer.parseInt(jobConf.getProperty(TgaDataGen.GENE_LENGTH));
		System.out.println("numGeneToGen is " + numGeneToGen);
		System.out.println("geneLength is " + numGeneToGen);
		StringValue fileName = (StringValue)val;
		String file;
		
		try {
			file = TwisterConfigurations.getInstance().getLocalDataDir() + "/" + fileName.toString();
			Random rand = new Random(System.nanoTime());
			//probability of setting 0 or 1 in the gene
			double rate = 0.5; 
			BufferedWriter writer = new BufferedWriter(new FileWriter(file));
			
			//first line should be the number of genes in the file
			String numGenes = numGeneToGen + "\n";
			writer.write(numGenes);
			//to actually generate the random genes
			for (long i = 0; i < numGeneToGen; i++) {
				String strGene = "";
				for (int j = 0; j < geneLength; j++) {
					if (rand.nextDouble() < rate) {
						strGene += "0";
					} else {
						strGene += "1";
					}
				}
				writer.write(strGene);
				writer.write("\n");
			}
			writer.flush();
			writer.close();
		} catch (Exception e) {
			throw new TwisterException(e);
		} finally {}
	}
}
