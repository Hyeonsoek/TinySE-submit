package edu.hanyang.submit;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import edu.hanyang.indexer.BPlusTree;

public class TinySEBPlusTree implements BPlusTree{

   int fanout;
   Tree tree;
   
   @Override
   public void close() {
      // TODO Auto-generated method stub
      
   }

   @Override
   public void insert(int arg0, int arg1) {
      // TODO Auto-generated method stub
      
      recurInsert(arg0, arg1, tree);
      
   }
   
   public void recurInsert(int arg0, int arg1, Tree tree) {
      
      if(tree.leaf) {
         
         
         
      }
      
      else {
         
         
      }
      
   }

   @SuppressWarnings({ "unused", "resource" })
   @Override
   public void open(String meta, String source, int blocksize, int nblocks)  {
      // TODO Auto-generated method stub
      
	  /*
      ClassLoader classloader = this.getClass().getClassLoader();
      
      File metaFile = new File(meta);
      File sourceFile = new File(source);
      File dataFile = new File(classloader.getResource("stage3-15000000.data").getFile());
      int fanout = blocksize / 4 / 2;
      
      Scanner metaScanner = new Scanner(metaFile);
      RandomAccessFile dataRandomAccess = new RandomAccessFile(dataFile, "r");
      
      byte[] tempBytes = new byte[blocksize];
      int metaRootp = metaScanner.nextInt();
      
      dataRandomAccess.read(tempBytes);
      
      tree.setRoot(tempBytes);
      */
   }

   @Override
   public int search(int arg0) {
      
      return recurSearch( arg0, tree );
      
   }
   
   public int recurSearch(int arg0, Tree tree) {
      
      if(tree.leaf == true) {
         
         int idx = 0;
         
         if(tree.keys.contains(arg0))
            idx = tree.keys.indexOf(arg0);
         
         return idx;
         
      }
      
      if(tree.getKey(0) > arg0)
         return recurSearch(arg0 , tree.getChildren(0));
      
      else if (tree.getKey(fanout) <= arg0)
         return recurSearch(arg0 , tree.getChildren(fanout+1));
      
      else {
         
         int front, end;
         
         for(int idx = 1; idx < fanout; idx++) {
            
            front = tree.getKey(idx);
            end = tree.getKey(idx+1);
            
            if( front <= arg0 && arg0 < end  ) {
               return recurSearch( arg0, tree.getChildren(idx) );
            }
         }
      }
      
      return 0;
   }
   
   public class Tree {
      
      boolean leaf;
      List<Integer> keys = new ArrayList<Integer>();
      List<Tree> childrens = new ArrayList<Tree>();
      
      public void setRoot(byte[] bytes, int blocksize) {
         
         ByteBuffer temp = ByteBuffer.wrap(bytes);
         
         
      }
      
      public int getKey(int idx) {
         return keys.get(idx);
      }
      
      public Tree getChildren(int idx) {
         return childrens.get(idx);
      }
   }
}