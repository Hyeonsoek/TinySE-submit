package edu.hanyang.submit;

import java.io.*;
import java.util.*;
import java.nio.ByteBuffer;

import edu.hanyang.indexer.BPlusTree;

public class TinySEBPlusTree implements BPlusTree{

	int blocksize;
	int nblocks;
	int fanout;
	int listSize;
	
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
	
	public void writNode(Node node) {
		
		
		ByteBuffer metabb;
		
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
	   	fanout = ((blocksize / 4) - 3) / 2;
	   	root = new Node();
   	}

   	@Override
   	public int search(int arg0) {
   		
   		Node node = findNode(arg0);
   		
   		LinkedList<Integer> tmpKey = node.getkeys();
   		LinkedList<Integer> tmpPtr = node.getpointer();
   		
   		for(int idx = 0; idx < fanout; idx++) {
   			
   			if(arg0 == tmpKey.get(idx)) {
   				
   				try {
   					
   					treeAccess.seek(tmpPtr.get(idx));
   					return treeAccess.readInt();
   					
   				}
   				
   				catch (IOException e) {
   					e.getStackTrace();
   				}
   			}
   			
   		}
   		
   		return -1;
   	}
   	
   	public Node searchNode(int key) {
   		
   		Node ret = null;
   		
   		for(Node node = root; node.leaf == 1; ret = node) {
   			
   			LinkedList<Integer> keys = node.getkeys();
   			LinkedList<Integer> ptrs = node.getpointer();
   			
   			for(int idx = 0; idx < fanout; idx++) {
   				
   				if(key < keys.get(idx)) {
   					
   					node = findNode(ptrs.get(idx));
   					break;
   				
   				}
   				
   			}
   			
   			if(key >= keys.get(fanout-1))
   				node = findNode(ptrs.get(fanout));
   			
   		}
   		
   		
   		return ret;
   	}
   	
   	public Node findNode(int address) {
   		
   		Node node = null;
   		
   		try {
   			
   			LinkedList<Integer> ptrs = new LinkedList<Integer>();
			LinkedList<Integer> keys = new LinkedList<Integer>();
			
			metaAccess.seek(address);
			int keySize = treeAccess.readInt();
			int leaf = treeAccess.readInt();
			int parent = treeAccess.readInt();
			byte[] nodeValues = new byte[(keySize * 2 + 3) * 4];
			
			treeAccess.seek(address);
			treeAccess.read(nodeValues);
			ByteBuffer nodebb = ByteBuffer.wrap(nodeValues);
			
			// 데이터 인풋
			for(int idx = 0; idx < fanout; idx++) {
				ptrs.add(nodebb.getInt());
				keys.add(nodebb.getInt());
			}
			ptrs.add(nodebb.getInt());
   			
			node = new Node(keys,ptrs, leaf, parent);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
   		
   		
   		return node;
   	}
   	
   
   	public class Node {
      
   		// leaf : 리프노드인지 확인 하기
   		// parent : 부모의 파일상 위치
   		// pointer : 자식 노드 파일상 위치값
   		// pointer의 0번째가 Node의 위치임
   		// keys : key들의 값
   		int leaf;
   		int parent;
   		LinkedList<Integer> pointer;
   		LinkedList<Integer> keys;
   		
   		public Node() {
   			this.leaf = 1;
   			this.parent = -1;
   			this.keys = new LinkedList<Integer>();
   			this.pointer = new LinkedList<Integer>();
   		}
   		
   		public Node(LinkedList<Integer> keys, LinkedList<Integer> ptr,  int leaf, int parent) {
   			this.parent = parent;
   			this.leaf = leaf;
   			this.pointer = ptr;
   			this.keys = keys;
   		}
   		
   		public void setKeys(LinkedList<Integer> keys) {
   			this.keys = keys;
   		}
   		
   		public void setPointer(LinkedList<Integer> pointer) {
   			this.pointer = pointer;
   		}
   		
   		public void setleaf(int leaf) {
   			this.leaf = leaf;
   		}
   		
   		public LinkedList<Integer> getpointer() {
   			return pointer;
   		}
   		
   		public LinkedList<Integer> getkeys() {
   			return keys;
   		}
   	}
}