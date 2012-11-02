package edu.indiana.cs.salsa.twisterga;

import java.util.BitSet;
import java.util.Random;

import cgl.imr.base.SerializationException;
import cgl.imr.base.TwisterMessage;
import cgl.imr.base.Value;

public class Gene implements Value {
	//private static int ID = 0;
	public Gene(int init_size) {
		//geneId = ID++;
		key = new BitSet(init_size);
		Random randGen = new Random();
		for (int i = 0; i < init_size; i++) {
			//magic number 0.5: to randomly populate the bitset, if lt 0.5 then the specific bit is true, otherwise it's false.
			if (randGen.nextDouble() < 0.5)
				key.set(i);
			else
				key.clear(i);
		}
		//calcFitValue();
	}
	
	public Gene(BitSet bs) {
		key = bs;
		//calcFitValue();
	}
	
	public Gene() {
		this(0);
	}
	
	public Gene(TwisterMessage message) throws SerializationException {
		this(0);
		fromTwisterMessage(message);
	}
	
	private BitSet key;	
	private int fitValue;
	private int geneId;
	
	/*
	private void calcFitValue() {
		int count = 0;
		long localKey = key;
		while (localKey != 0) {
			++count;
			localKey &= (localKey - 1);
		}
		fitValue = count;
	}*/
	
	public int getGeneId() {
		return geneId;
	}

	public void setGeneId(int geneId) {
		this.geneId = geneId;
	}

	public void calcFitValue() {
		fitValue = key.cardinality();		
	}
	
	public int getFitValue() { 
		return fitValue;
	} 
	
	public BitSet getKey() {
		return key;
	}
	
	/*
	public BitSet convertToBitSet() {
		BitSet bs = new BitSet(Long.SIZE);
		long numInt = key;
		int index = 0;
		while (numInt != 0L) {
			if ((numInt & 1L) != 0) {
				bs.set(index);
			}
			++index;
			numInt = numInt >>> 1;
		}
		return bs;
	}
	*/
	
	public void setKey(BitSet bs) {
		/*
		long res = 0;
		for (int i = 0; i < bs.length(); i++) {
			if (bs.get(i)) {
				res ^= (1L << i);
			}
		}
		*/
		key = bs;
		//calcFitValue();
	}
	
	@Override
	/**
	 * load object data from bytes
	 */
	public void fromTwisterMessage(TwisterMessage message)
			throws SerializationException {
		//System.out.println("reading from message begins");
		//System.out.println("gene message: " + message.toString());
		this.geneId = message.readInt();
		//System.out.println("geneID: " + geneId);
		this.fitValue = message.readInt();
		//System.out.println("fitValue: " + fitValue);
		int bytesLen = message.readInt();
		byte[] bytesOfBits = new byte[bytesLen];
		bytesOfBits = message.readBytes();
		/*
		for (int i = 0; i < bytesLen; i++) {
			System.out.println(bytesOfBits[i]);
		}
		*/
		key = new BitSet(bytesLen * Byte.SIZE);
		for (int i = 0; i < bytesLen * Byte.SIZE; i++) {
			if ((bytesOfBits[i/Byte.SIZE] & (1<<(i%8))) > 0) {
				key.set(i);
			}
		}
		//System.out.println("Gene: " + key.toString());
		//System.out.println("reading from message ends");
	}

	@Override
	/**
	 * transfer object data to bytes
	 */
	public void toTwisterMessage(TwisterMessage message)
			throws SerializationException {
		//System.out.println("Gene serialization begins");
		message.writeInt(this.geneId);
		message.writeInt(this.fitValue);
		
		byte[] bytesOfBits = new byte[key.size()/Byte.SIZE];
		int bytesLen = bytesOfBits.length;
		message.writeInt(bytesLen);
		for (int i = 0; i < key.length(); i++) {
			if (key.get(i)) {
				bytesOfBits[i/Byte.SIZE] |= 1<<(i%8);
			}
		}
		message.writeBytes(bytesOfBits);
		//System.out.println("Gene serialization ends");
	}
	

}
