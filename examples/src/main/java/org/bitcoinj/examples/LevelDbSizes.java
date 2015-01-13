package org.bitcoinj.examples;

import static org.iq80.leveldb.impl.Iq80DBFactory.*;

import java.io.File;
import java.io.IOException;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.Range;

public class LevelDbSizes {

	public static void main(String[] args) throws Exception {
		DB db=null;
		Options options = new Options();
		options.createIfMissing(false);
		try {
			db = factory.open(new File(args[0]),options);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Thread.sleep(10000);
		for(int i=0;i<10;i++){
			byte[] from=new byte[1];
			from[0]=(byte)i;
			byte[] to=new byte[1];
			to[0]=(byte)(i+1);
			
			long[] sizes = db.getApproximateSizes(new Range(from,to));
			System.out.println("From:"+i+" to:"+(i+1)+" Size: "+sizes[0]);	
		}
		byte[] from=new byte[1];
		from[0]=0;
		byte[] to=new byte[1];
		to[0]=-128;
		
		long[] sizes = db.getApproximateSizes(new Range(from,to));
		System.out.println("Size: "+sizes[0]);	
		db.close();
	}

}
