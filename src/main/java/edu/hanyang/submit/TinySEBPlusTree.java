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
		
		try {
			metaAccess.close();
			treeAccess.close();
		}
		catch (IOException e) {
			e.getStackTrace();
		}
	
	}

	@Override
	public void insert(int key, int value) {
	
		Node insert = searchNode(key);
		
		LinkedList<Integer> keys = insert.getkeys();
		LinkedList<Integer> ptrs = insert.getpointer();
		int lastidx = 0;
		int keySize = keys.size();
		
		for(int idx = 0; idx < keySize; ++idx) {
			if(key < keys.get(idx)) {
				keys.add(idx, key);
				lastidx = idx;
				break;
			}
		}
		
		if(key > keys.get(keySize-1)) {
			lastidx = keySize;
			keys.add(key);
		}
		
		insert.setKeys(keys);
		
		if(keySize + 1 > fanout) {
			splitNode(insert);
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
	   	fanout = ((blocksize / 4) - 3) / 2;
	   	root = new Node();
   	}

   	@Override
   	public int search(int arg0) {
   		
   		Node node = searchNode(arg0);
   		
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
			
			// ������ ��ǲ
			for(int idx = 0; idx < keySize; idx++) {
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
   	
	public void writNode(Node node) {
		
		ByteBuffer metabb;
		
	}
   
   	public class Node {
      
   		// leaf : ����������� Ȯ�� �ϱ�
   		// parent : �θ��� ���ϻ� ��ġ
   		// pointer : �ڽ� ��� ���ϻ� ��ġ��
   		// pointer�� 0��°�� Node�� ��ġ��
   		// keys : key���� ��
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