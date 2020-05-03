package edu.hanyang;

import static org.junit.Assert.*;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URLDecoder;

import org.junit.Before;
import org.junit.Test;

import edu.hanyang.submit.TinySEExternalSort;

public class ExternalSortTest {
	@Before
	public void init() {
		clean("./tmp");
		File resultFile = new File("./sorted.data");
		if(resultFile.exists()) {
			resultFile.delete();
		}
	}
	
	@Test
	public void TestSort() throws IOException {
		int blocksize = 4096;
		int nblocks = 1000;
		ClassLoader classLoader = this.getClass().getClassLoader();
		File infile = new File(classLoader.getResource("test.data").getFile());
		
		String outfile = "./tmp/sorted.data";
		String tmpdir = "./tmp";
		File resultFile = new File(outfile);
		
		TinySEExternalSort sort = new TinySEExternalSort();
		long timestamp = System.currentTimeMillis();
		sort.sort(infile.getAbsolutePath(), outfile, tmpdir, blocksize, nblocks);
		System.out.println("time duration: " + (System.currentTimeMillis() - timestamp) + " msecs with " + nblocks + " blocks of size " + blocksize + " bytes");

		
		File answerFile = new File(classLoader.getResource("answer.data").getFile());
		DataInputStream resultInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(resultFile)));
		DataInputStream answerInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(URLDecoder.decode(answerFile.getAbsolutePath(),"UTF-8"))));

		assertNotNull(resultInputStream);
		assertNotNull(answerInputStream);
		
		try {
			for (int i = 0; i < 100000; i++) {
				assertEquals(resultInputStream.readInt(), answerInputStream.readInt());
				assertEquals(resultInputStream.readInt(),answerInputStream.readInt());
				assertEquals(resultInputStream.readInt(),answerInputStream.readInt());
			}
		} catch (EOFException e) {
			e.getStackTrace();
		}
		
		resultInputStream.close();
		answerInputStream.close();
	}

	private void clean(String dir) {
		File file = new File(dir);
		File[] tmpFiles = file.listFiles();
		if (tmpFiles != null) {
			for (int i = 0; i < tmpFiles.length; i++) {
				if (tmpFiles[i].isFile()) {
					tmpFiles[i].delete();
				} else {
					clean(tmpFiles[i].getAbsolutePath());
				}
				tmpFiles[i].delete();
			}
			file.delete();
		}
	}
}
