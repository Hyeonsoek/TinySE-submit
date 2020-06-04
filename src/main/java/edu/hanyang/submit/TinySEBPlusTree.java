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
	
		System.out.println("-----start insert ("+key+", "+value+")------");
		
		if(root.getkeys().size() == 0) {
			
			System.out.println("-----first insert-----------");

			LinkedList<Integer> keys = root.getkeys();
			LinkedList<Integer> ptrs = root.getpointer();
			
			keys.add(key);
			ptrs.add(0);
			
			root.setKeys(keys);
			root.setPointer(ptrs);
			
			try {
				
				treeAccess.writeInt(value);
				writeNode(root);
				
			} catch (IOException e) {
				
				System.out.println("This is insert #52");
				System.out.println(e);
				
			}
			
			System.out.println("-----first end-----");
			
			return;
		}
		
		Node insert = root.leaf == 1 ? root : searchNode(key);
		
		System.out.println("NICE");
		printNode(insert);
		System.out.println("NICE T");
		
		LinkedList<Integer> keys = insert.getkeys();
		LinkedList<Integer> ptrs = insert.getpointer();
		int lastidx = -1;
		int keySize = keys.size();
		
		for(int idx = 0; idx < keySize; ++idx) {
			if(key < keys.get(idx)) {
				keys.add(idx, key);
				lastidx = idx;
				break;
			}
		}
		
		if(key >= keys.get(keySize-1)) {
			lastidx = keySize;
			keys.add(key);
		}
		
		try {
			
			treeAccess.seek(treeAccess.length());
			treeAccess.writeInt(value);
			int pointer = (int)treeAccess.getFilePointer();
			
			if(lastidx == keySize)
				ptrs.add(pointer);
			else ptrs.add(lastidx, pointer);
			
			insert.setKeys(keys);
			insert.setPointer(ptrs);
			
			if(insert.parent != -1) {
				
				for(Node node = findNode(insert.parent); ;) {
					
					LinkedList<Integer> tmpptrs = node.getpointer();
					int size = tmpptrs.size();
					for(int i=0; i<size; i++) {
						if(insert.pos == tmpptrs.get(i)) {
							treeAccess.seek(node.pos + (i * 2 + 3) * 4);
							treeAccess.writeInt(pointer);
							
							if(node.parent == -1) 
								root = findNode(node.pos);
							break;
						}
					}
					
					if(node.parent != -1)
						node = findNode(node.parent);
					else break;
				}
				
			}
			
			insert.setposition(pointer);
			
			System.out.println("NICE");
			printNode(insert);
			System.out.println("NICE T");
			
			if(keySize + 1 > fanout) {
				split(insert);
			} else {
				writeNode(insert);
			}
			
		}
		catch (IOException e) {
			
			System.out.println("This is insert #110");
			System.out.println(e);
			
		}
		
		System.out.println("-----Insert End-----");
	}
	
	public void split(Node node) {
		
		System.out.println("-----split start-----");
		Node split = new Node();
		
		if(node.parent == -1 && node.leaf == 1) {
			
			splitNode(node, split);
			
			Node parent = makeParent(node, split);
			root = parent;
			
		}
		else {
			
			while(node.getkeys().size() > fanout) {
				
				splitNode(node, split);
				
				if(node.parent == -1) {
					splitNode(node, split);
					
					Node parent = makeParent(node, split);
					root = parent;
					
					break;
				}
				
				Node parent = findNode(node.parent);
				
				LinkedList<Integer> pptrs = parent.getpointer();
				LinkedList<Integer> pkeys = parent.getkeys();
				int pkeySize = pkeys.size();
				int splitPtr = split.getpointer().get(0);
				int splitKey = split.getkeys().get(0);
					
				for(int idx = 0; idx < pkeySize; idx++) {
					if(splitKey < pkeys.get(idx)) {
						pptrs.add(idx, splitPtr);
						pkeys.add(idx, splitKey);
					}
				}
				
				if(splitKey > pkeys.get(pkeySize-1)) {
					pptrs.add(splitPtr);
					pkeys.add(splitKey);
				}
				
				
				parent.setKeys(pkeys);
				parent.setPointer(pptrs);
					
				try {
					treeAccess.seek(treeAccess.length());
					parent.setposition((int) treeAccess.getFilePointer());
					writeNode(parent);
				} catch (IOException e) {
					
					System.out.println("This is split #165");
					System.out.println(e);
					
				}
					
				node = parent;
				split = new Node();
			}
		}
		
		System.out.println("-----split End-----");
	}
	
	public Node makeParent(Node node, Node splitNode) {
		
		System.out.println("-----makeParent start-----");
		
		Node parent = new Node();
		
		LinkedList<Integer> pptrs = parent.getpointer();
		LinkedList<Integer> pkeys = parent.getkeys();
		
		try {
			
			treeAccess.seek(treeAccess.length());
			node.setposition((int)treeAccess.getFilePointer());
			writeNode(node);
			
			splitNode.setposition((int)treeAccess.getFilePointer());
			writeNode(splitNode);
			
			pptrs.add(node.pos);
			pptrs.add(splitNode.pos);
			
			pkeys.add(splitNode.getkeys().get(0));
			
			parent.setKeys(pkeys);
			parent.setPointer(pptrs);
			parent.setleaf(0);
			parent.setposition((int)treeAccess.getFilePointer());
			writeNode(parent);
			
			changeParentValueNode(node.pos, parent.pos);
			changeParentValueNode(splitNode.pos, parent.pos);
		}
		catch (IOException e) {
			
			System.out.println("This is makeParent #212");
			System.out.println(e);
			
		}
		
		System.out.println("-----makeParent End-----");
		return parent;
	}
	
	public void splitNode(Node node, Node splitNode) {
		
		System.out.println("-----splitNode start-----");
		LinkedList<Integer> ptrs = node.getpointer();
		LinkedList<Integer> keys = node.getkeys();
		
		LinkedList<Integer> splitptrs = splitNode.getpointer();
		LinkedList<Integer> splitkeys = splitNode.getkeys();
		
		int keySize = keys.size();
		
		for(int nodeIdx = keySize/2 + 1; nodeIdx < keySize; nodeIdx++) {
			splitptrs.add(ptrs.get(nodeIdx));
			splitkeys.add(keys.get(nodeIdx));
		}
		if(node.leaf == 0)
			splitptrs.add(ptrs.get(keySize-1));
		
		for(int nodeIdx = keySize/2 + 1; nodeIdx < keySize; nodeIdx++) {
			ptrs.remove(ptrs.size()-1);
			keys.remove(keys.size()-1);
		}
		if(node.leaf == 0)
			ptrs.remove(ptrs.size()-1);
		
		node.setKeys(keys);
		node.setPointer(ptrs);
		splitNode.setKeys(splitkeys);
		splitNode.setPointer(splitptrs);
		
		System.out.println("-----splitNode End-----");
	}

	@Override
	public void open(String meta, String source, int blocksize, int nblocks)  {
	   
		//TODO Auto-generated method stub
		
		File temp = new File("./tmp");
		File metaFile = new File(meta);
		File sourceFile = new File(source);
	   
		try {
			temp.mkdir();
			
			metaAccess = new RandomAccessFile(metaFile, "rw");
			treeAccess =  new RandomAccessFile(sourceFile, "rw");
			
		} catch (IOException e) {
			
			System.out.println("This is open #238");
			System.out.println(e);
			
		}

	   	this.blocksize = blocksize;
	   	this.nblocks = nblocks;
	   	fanout = ((blocksize / 4) - 4) / 2;
	   	root = new Node();
	   	
	   	System.out.println("-----open OK--------");
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
   					
   					System.out.println("This is search #267");
   					System.out.println(e);
   					
   				}
   			}
   		}
   		
   		return -1;
   	}
   	
   	public Node searchNode(int key) {
   		
   		Node ret = null;
   		System.out.println("-----SearchNode : " + key + "-----");
   		
   		for(Node node = root; node.leaf == 0; ret = node) {
   			
   			LinkedList<Integer> keys = node.getkeys();
   			LinkedList<Integer> ptrs = node.getpointer();
   			
   			int keySize = keys.size();
   			for(int idx = 0; idx < keySize; idx++) {
   				if(key < keys.get(idx)) {
   					node = findNode(ptrs.get(idx));
   					break;
   				}
   			}
   			
   			if(key >= keys.get(keySize-1))
   				node = findNode(ptrs.get(keySize));
   			
   		}
   		
   		return ret;
   	}
   	
   	public Node findNode(int address) {
   		
   		Node node = null;
   		System.out.println("-----findNode start : address : " + address + "-----");
   		
   		try {
   			
   			LinkedList<Integer> ptrs = new LinkedList<Integer>();
			LinkedList<Integer> keys = new LinkedList<Integer>();
			
			treeAccess.seek(address);
			int keySize = treeAccess.readInt();
			int leaf = treeAccess.readInt();
			int parent = treeAccess.readInt();
			
			System.out.println("( keySize : " + keySize + ", leaf : " + leaf + ", parent : " + parent + ")");

			// 데이터 인풋
			for(int idx = 0; idx < keySize; idx++) {
				ptrs.add(treeAccess.readInt());
				keys.add(treeAccess.readInt());
			}

			if(leaf == 0)
				ptrs.add(treeAccess.readInt());
   			
			node = new Node(keys, ptrs, leaf, parent, address);
			
		} catch (IOException e) {
			
			System.out.println("This is findNode #414");
			System.out.println(e);
			
		}
   		
   		System.out.println("-----findNode End-----");
   		return node;
   	}
   	
	public void writeNode(Node node) {
		
		System.out.println("-----writeNode start-----");
		
		int maxByte = (node.getkeys().size()*2 + 3) * 4;
		if(node.leaf == 0)
			maxByte += 4;
		
		byte[] nodeValue = new byte[maxByte];
		ByteBuffer nodeBuf = ByteBuffer.wrap(nodeValue);
		
		try {
			node.setposition((int)treeAccess.length());
			treeAccess.seek(node.pos);
			
			nodeBuf.putInt(node.getkeys().size());
			nodeBuf.putInt(node.leaf);
			nodeBuf.putInt(node.parent);
			
			LinkedList<Integer> ptrs = node.getpointer();
			LinkedList<Integer> keys = node.getkeys();
			
			int keysSize = keys.size();
			
			for(int idx = 0; idx < keysSize; idx++) {
				nodeBuf.putInt(ptrs.get(idx));
				nodeBuf.putInt(keys.get(idx));
			}
			
			if(node.leaf == 0)
			{ 
				System.out.println(ptrs.get(keysSize));
				nodeBuf.putInt(ptrs.get(keysSize));
			}
			
			treeAccess.write(nodeValue);
			
		}
		catch(IOException e) {
			
			System.out.println("This is writeNode #361");
			System.out.println(e);
			
		}
		
		System.out.println("-----writeNode End-----");
	}
	
	public void changeParentValueNode(int targetAddress, int value) {
		
		try {
			
			treeAccess.seek(targetAddress);
			treeAccess.readInt();
			treeAccess.readInt();
			
			treeAccess.writeInt(value);
			
		} catch (IOException e) {
			
			System.out.println("This is changeParentValueNode #417");
			System.out.println(e);
			
		}
		
	}
	
	public void printNode(Node node) {
		
		System.out.println("-----printNode Start-----");
		System.out.println("(keySize : " + node.getkeys().size() + ", pos : " + node.pos + ", parent : " + node.parent + ")");
		for(int i=0; i<node.getkeys().size(); i++)
			System.out.println("("+node.getkeys().get(i)+", "+node.getpointer().get(i) +")");
		if(node.leaf == 0)
			System.out.println("(0, "+node.getpointer().get(node.getpointer().size()-1) + ")");
		
		System.out.println("-----printNode End-------");
		
	}
	
   	public class Node {
      
   		// leaf : 리프노드인지 확인 하기
   		// parent : 부모의 파일상 위치
   		// pointer : 자식 노드 파일상 위치값
   		// pointer의 0번째가 Node의 위치임
   		// keys : key들의 값
   		int leaf;
   		int parent;
   		int pos;
   		LinkedList<Integer> pointer;
   		LinkedList<Integer> keys;
   		
   		public Node() {
   			this.leaf = 1;
   			this.parent = -1;
   			this.pos = -1;
   			this.keys = new LinkedList<Integer>();
   			this.pointer = new LinkedList<Integer>();
   		}

		public Node(LinkedList<Integer> keys, LinkedList<Integer> ptr, int leaf, int parent, int pos) {
   			this.pos = pos;
   			this.parent = parent;
   			this.leaf = leaf;
   			this.pointer = ptr;
   			this.keys = keys;
   		}
		
   		public void setparent(int parent) {
   			this.parent = parent;
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
   		
   		public void setposition(int pos) {
   			this.pos = pos;
   		}
   		
   		public LinkedList<Integer> getpointer() {
   			return pointer;
   		}
   		
   		public LinkedList<Integer> getkeys() {
   			return keys;
   		}
   	}
}