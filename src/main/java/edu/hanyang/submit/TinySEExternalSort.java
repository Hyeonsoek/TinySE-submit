package edu.hanyang.submit;

import java.io.*;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import edu.hanyang.indexer.ExternalSort;

public class TinySEExternalSort implements ExternalSort {

	private Triple<Integer, Integer, Integer> triple;

	public void sort(String infile, String outfile, String tmpdir, int blocksize, int nblocks) throws IOException {
		// complete the method
		infile = URLDecoder.decode(infile, "UTF-8");
		outfile = URLDecoder.decode(outfile, "UTF-8");
		
		File tmpfile = new File(tmpdir);
		File file = new File(infile);
		File initfile = new File(tmpdir+"/init");
			
		tmpfile.mkdir();
		initfile.mkdir();
		
		DataInputStream is = new DataInputStream( new BufferedInputStream(
					new FileInputStream(infile), blocksize
				)
			);
		
		int blockNum = (int) Math.ceil((float)file.length() / blocksize);
		int fileNum = (int) Math.ceil((float)blockNum / nblocks);
		int intblocks = blocksize/4;
		
		try {
			for(int index = 0; index < fileNum; ++index) {
				String runs =  tmpdir + "/init/runs_" + (index < 10 ? "0" + index : index) + ".data";
				File tmpFile = new File(runs);
				DataOutputStream tmpOs = new DataOutputStream( new BufferedOutputStream (
							new FileOutputStream(tmpFile), blocksize)
						);
				
				List< Triple<Integer,Integer,Integer> > list = 
						new ArrayList< Triple<Integer,Integer,Integer> >();
				
				for(int j=0; j<nblocks; ++j) {
					for(int i = 0; i<intblocks/3; ++i) {
						if(is.available() > 0)
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
		
		try {
			mergingAll(fileNum, blocksize, nblocks, tmpdir, outfile);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		is.close();
	}
	
	public Triple<Integer, Integer, Integer> readTripleInt(DataInputStream is) throws IOException {
		
		Triple<Integer, Integer, Integer> ret = null;
		
		try {
			ret = Triple.of(is.readInt(), is.readInt(), is.readInt());
		} catch (EOFException e) {
			e.getStackTrace();
		}
		
		return ret;
	}

	public void twoWayMerge(int blocksize, int nblocks, String first, String second, String out) throws Exception {
		
		File firstFile = new File(first);
		File secondFile = new File(second);
		File outFile = new File(out);
		
		DataOutputStream os = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outFile), blocksize));
		DataInputStream firIS = new DataInputStream(new BufferedInputStream(new FileInputStream(firstFile), blocksize));
		
		PriorityQueue<Pair<Triple<Integer, Integer, Integer>, Integer> > list = new PriorityQueue<Pair< Triple<Integer, Integer, Integer>, Integer>>();
		
		if(secondFile.exists()) {
			DataInputStream secIS = new DataInputStream(new BufferedInputStream(new FileInputStream(secondFile), blocksize));
			
			try {
				int count = blocksize/12;
				int fircount = count;
				int seccount = count;
				
				for(int j=0; j<count; j++) {
						list.add(Pair.of(readTripleInt(firIS), 1));
						list.add(Pair.of(readTripleInt(secIS), 2));
				}
				
				while(!list.isEmpty() && fircount != 0 && seccount != 0)
				{
					Pair<Triple<Integer, Integer, Integer>, Integer> temp = list.poll();
					
					if(temp.getRight() == 1) {
						fircount--;
						if(firIS.available() > 0) {
							list.add(Pair.of(readTripleInt(firIS), 1));
							fircount++;
						}
					}
					else {
						seccount--;
						if(secIS.available() > 0) {
							list.add(Pair.of(readTripleInt(secIS), 2));
							seccount++;
						}
					}
					
					os.writeInt(temp.getLeft().getLeft());
					os.writeInt(temp.getLeft().getMiddle());
					os.writeInt(temp.getLeft().getRight());
				}
				os.flush();
				
			} catch (EOFException e) {
				e.getStackTrace();
			}
			
			secIS.close();
		} else {
			
			try {
				int count = blocksize/12;
				
				int fircount = count;
					
				for(int j=0; j<count; j++)
					list.add(Pair.of(readTripleInt(firIS),1));
				
				while(!list.isEmpty() && fircount != 0) {
					Pair< Triple<Integer, Integer, Integer>, Integer> temp = list.poll();
					
					fircount--;
					
					if(firIS.available() > 0) {
						fircount++;
						list.add(Pair.of(readTripleInt(firIS), 1));
					}
					
					os.writeInt(temp.getLeft().getLeft());
					os.writeInt(temp.getLeft().getMiddle());
					os.writeInt(temp.getLeft().getRight());
				}
				
				os.flush();
				list.clear();
				
			} catch (EOFException e) {
				e.getStackTrace();
			}
		}
		
		firIS.close();
		os.close();
	}
	
	public void mergingAll(int runsCount, int blocksize, int nblocks , String tmpdir, String outfile) throws Exception {
		
		int nfirstRuns = (int) Math.ceil((double)runsCount/2);
		for(int index = nfirstRuns, preidx = runsCount; index > 0 && preidx > 1; preidx=index,index = (int) Math.ceil((double)index/2)) {
			String passdir = tmpdir + "/" + index;
			File pass = new File(passdir);
			pass.mkdir();
			
			for(int runs = 0; runs < index; runs++) {
				String predir = (preidx == runsCount) ? "/init" : ("/" + preidx);
				String postdir1 = "/runs_" + (runs * 2 < 10 ? "0" + (runs*2) : runs*2) + ".data";
				String postdir2 = "/runs_" + (runs * 2 + 1 < 10 ? "0" + (runs*2 + 1) : (runs*2 + 1)) + ".data";
				
				String out = passdir + "/runs_" + (runs < 10 ? "0" + runs : runs) + ".data";
				String first = tmpdir + predir + postdir1;
				String second = tmpdir + predir + postdir2;
				
				if(index > 1)
				{ twoWayMerge(blocksize, nblocks, first, second, out); }
				else twoWayMerge(blocksize, nblocks, first, second, outfile);
			}
		}
	}
	
	public void readPrint(DataInputStream is) throws IOException {
		
		try {
			int left; int mid = 0; int right = 0;
			while(is.available() > 0) {
				left = is.readInt();
				mid = is.readInt();
				right = is.readInt();
				System.out.println(left+", "+mid+", "+right);
			}
		} catch (EOFException e) {
			e.getStackTrace();
		}
	}
}