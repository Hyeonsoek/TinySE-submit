package edu.hanyang.submit;

import java.io.*;
import java.util.*;
import java.nio.ByteBuffer;

import edu.hanyang.indexer.BPlusTree;

public class TinySEBPlusTree implements BPlusTree{

	int blocksize;
	int nblocks;
	int fanout;
	int lastInput = 0;
	
	Node root;
	RandomAccessFile metaAccess;
	RandomAccessFile treeAccess;
   
	@Override
	public void close() {
		// TODO Auto-generated method stub
		try {
			treeAccess.close();
		} catch (IOException e) {
			System.out.println(e);
		}
	}

	@Override
	public void insert(int key, int value) {
		// TODO Auto-generated method stub
		
		LinkedList<Integer> tempKeys;
		LinkedList<Integer> tempPtr;
		
		Node find = findNode(key);
		
		tempKeys = find.getkeys();
		tempPtr = find.getpointer();

		int keysSize = tempKeys.size();
		int tmppos = 0;
		
		for(int index = 0; index < keysSize-1; index++) {
			if( key < tempKeys.get(index) ) {
				tempKeys.add(index, key);
				
				if(find.leaf)
					tempPtr.add(index + 1, tmppos = find.getPosition() + (index+1) * 4);
				break;
			}
		}
		
		if(tempKeys.size() > this.fanout)
			splitNode(find);
		
		try {
			
			treeAccess.seek(tmppos);
			treeAccess.writeInt(value);
			
		} catch(IOException e) {
			
			System.out.println(e);
		
		}
		
	}
	
	public void splitNode(Node node) {
		
		
		
	}

	@Override
	public void open(String meta, String source, int blocksize, int nblocks)  {
	   
		//TODO Auto-generated method stub
		File metaFile = new File(meta);
		File sourceFile = new File(source);
	   
		try {
			if( !metaFile.exists() )
				metaFile.createNewFile();
			if( !sourceFile.exists() )
				sourceFile.createNewFile();
			
			metaAccess = new RandomAccessFile(sourceFile, "rw");
			treeAccess =  new RandomAccessFile(sourceFile, "rw");
			
		} catch (IOException e) {
			System.out.println(e);
		}

	   	this.blocksize = blocksize;
	   	this.nblocks = nblocks;
	   	this.fanout = blocksize / 4 / 2;
	   	root = new Node();
   	}

   	@Override
   	public int search(int arg0) {
   		
   		Node node = findNode(arg0);
   		
   		LinkedList<Integer> tmpKey = node.getkeys();
   		LinkedList<Integer> tmpPtr = node.getpointer();
   		
   		int keySize = tmpKey.size();
   		for(int idx = 0; idx < keySize; idx++) {
   			
   			if(arg0 == tmpKey.get(idx)) {
   				
   				int ret = 0;
   				int seek = tmpPtr.get(idx);
   				
   				try {
   					
   					treeAccess.seek(seek);
   					ret = treeAccess.readInt();
   					
   				}
   				catch (IOException e) {
   					System.out.println(e);
   				}
   				
   				return ret;
   			
   			}
   			
   		}
   		
   		return -1;
   	}
   	
   	public Node findNode(int key) {
   		
   		Node ret = null;
   		
   		for(Node node = root; node.leaf;) {
   			
   			LinkedList<Integer> keys = node.getkeys();
   			LinkedList<Node> child = node.getchild();
   			boolean last = true;
   			
   			int keysSize = keys.size();
   			for(int idx = 0; idx < keysSize; idx++) {
   				
   				if(key < keys.get(idx)) {
   					node = child.get(idx);
   					last = false;
   				}
   				
   			}
   			
   			if(last) 
   				node = child.get(keysSize + 1);
   			
   		}
   		
   		return ret;
   	}
   
   	public class Node {
      
   		// position : 이 노드의 파일상 위치
   		// leaf : 리프노드인지 확인 하기
   		// child : 아이 노드들의 위치
   		// keys : key들의 값
   		int position;
   		boolean leaf;
   		Node parent;
   		LinkedList<Integer> pointer;
   		LinkedList<Integer> keys;
   		LinkedList<Node> child;
   		
   		public Node() {
   			this.position = 0;
   			this.leaf = true;
   			this.keys = new LinkedList<Integer>();
   			this.pointer = new LinkedList<Integer>();
   			this.child = new LinkedList<Node>();
   			this.parent = null;
   		}
   		
   		public void setKeys(LinkedList<Integer> keys) {
   			this.keys = keys;
   		}
   		
   		public void setPointer(LinkedList<Integer> pointer) {
   			this.pointer = pointer;
   		}
   		
   		public void setleaf(boolean leaf) {
   			this.leaf = leaf;
   		}
   		
   		public int getPosition() {
   			return position;
   		}
   		
   		public LinkedList<Integer> getpointer() {
   			return pointer;
   		}
   		
   		public LinkedList<Integer> getkeys() {
   			return keys;
   		}
   		
   		public LinkedList<Node> getchild() {
   			return child;
   		}
   	}
}