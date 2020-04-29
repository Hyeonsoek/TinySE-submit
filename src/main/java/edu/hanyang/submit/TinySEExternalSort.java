package edu.hanyang.submit;

import java.io.*;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.commons.lang3.tuple.Triple;

import edu.hanyang.indexer.ExternalSort;

public class TinySEExternalSort implements ExternalSort {

	public void sort(String infile, String outfile, String tmpdir, int blocksize, int nblocks) throws IOException {
		// complete the method
		infile = URLDecoder.decode(infile, "UTF-8");
		outfile = URLDecoder.decode(outfile, "UTF-8");
		File tmpfile = new File(tmpdir);
			
		try {
			tmpfile.mkdir();
		} catch (Exception e) {
			e.getStackTrace();
		}
		
		File outputFile = new File(outfile);
		
		DataInputStream is = new DataInputStream( new BufferedInputStream(
					new FileInputStream(infile), blocksize
				)
			);
		DataOutputStream os = new DataOutputStream( new BufferedOutputStream(
					new FileOutputStream(outputFile), blocksize
				)
			);
		
		File file = new File(infile);
		int blockNum = (int) Math.ceil((float)file.length() / blocksize);
		int fileNum = (int) Math.ceil((float)blockNum / nblocks);
		int intblocks = blocksize/4;
		
		try {
			for(int index = 0; index < fileNum; ++index) {
				String runs =  tmpdir + "/runs_" + (index < 10 ? "0" + index : index) + ".data";
				File tmpFile = new File(runs);
				DataOutputStream tmpOs = new DataOutputStream( new BufferedOutputStream (
							new FileOutputStream(tmpFile), blocksize)
						);
				
				List< Triple<Integer,Integer,Integer> > list = 
						new ArrayList< Triple<Integer,Integer,Integer> >();
				
				for(int j=0; j<nblocks; ++j) {
					for(int i = 0; i<intblocks/3; ++i) {
						list.add(readTripleInt(is));
					}
				}
				
				list.sort(new Comparator< Triple<Integer,Integer,Integer> >() {

					@Override
					public int compare(Triple<Integer, Integer, Integer> o1, Triple<Integer, Integer, Integer> o2) {
						if(o1.getLeft() < o2.getLeft())
							return -1;
						else if(o1.getLeft() > o2.getLeft())
							return 1;
						else {
							if(o1.getMiddle() < o2.getMiddle())
								return -1;
							if(o1.getMiddle() > o2.getMiddle())
								return 1;
							else {
								if(o1.getRight() < o2.getRight())
									return -1;
								else if(o1.getRight() > o2.getRight())
									return 1;
								else return 0;
							}
						}
					}
				});
				
				for(Triple<Integer,Integer,Integer> e : list) {
					tmpOs.writeInt(e.getLeft());
					tmpOs.writeInt(e.getMiddle());
					tmpOs.writeInt(e.getRight());
				}
				
				tmpOs.flush();
				tmpOs.close();
			}
		} catch (Exception e) {
			e.getStackTrace();
		}
		
		is.close();
		os.close();
	}
	
	public Triple<Integer, Integer, Integer> readTripleInt(DataInputStream is) throws IOException {
		
		Triple<Integer, Integer, Integer> ret = null;
		
		try {
			int right = is.readInt();
			int middle = is.readInt();
			int left = is.readInt();
			
			ret = Triple.of(right, middle, left);
			
		} catch (EOFException e) {
			e.getStackTrace();
		}
		
		return ret;
	}
	
	public void nWayExternal(int n, int blocksize, String tmp , DataOutputStream os) throws IOException {
		
		PriorityQueue< Triple<Integer, Integer, Integer> > minHeap = new PriorityQueue();
		
		File tmpdir = new File(tmp);
		File[] tmplist = tmpdir.listFiles();
		
		
	}
}