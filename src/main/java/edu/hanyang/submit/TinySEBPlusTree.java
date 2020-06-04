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
		
		if(key > keys.get(keySize-1)) {
			lastidx = keySize;
			keys.add(key);
		}
		
		try {
			
			treeAccess.seek(treeAccess.length());
			treeAccess.writeInt(value);
			int pointer = (int) (treeAccess.getFilePointer() - 4);
			
			if(lastidx == keySize)
				ptrs.add(pointer);
			else ptrs.add(lastidx, pointer);
			
			insert.setKeys(keys);
			insert.setPointer(ptrs);
			
			if(keySize + 1 > fanout) {
				split(insert);
			} else {
				writeNode(insert);
			}
			
		}
		catch (IOException e) {
			
			System.out.println("This is insert #97");
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
				
				writeNode(parent);
				
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
		
		pptrs.add(node.pos);
		pptrs.add(splitNode.pos);
		
		pkeys.add(splitNode.getkeys().get(0));
		
		parent.setKeys(pkeys);
		parent.setPointer(pptrs);
		parent.setleaf(0);
		
		try {
			parent.setposition((int) treeAccess.length());
			writeNode(parent);
		}
		catch (IOException e) {
			
			System.out.println("This is makeParent #175");
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
			ptrs.remove();
			keys.remove();
		}
		if(node.leaf == 0)
			ptrs.remove();
		
		node.setKeys(keys);
		node.setPointer(ptrs);
		splitNode.setKeys(splitkeys);
		splitNode.setPointer(splitptrs);
		
		try {
			treeAccess.seek(treeAccess.length());
			writeNode(node);
			writeNode(splitNode);
		}
		catch (IOException e) {
			
			System.out.println("This is splitNode() #213 ");
			System.out.println(e);
			
		}
		
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
   		System.out.println("-----SearchNode-----");
   		
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
   		System.out.println("-----findNode start : address : "+ address +"-----");
   		
   		try {
   			
   			LinkedList<Integer> ptrs = new LinkedList<Integer>();
			LinkedList<Integer> keys = new LinkedList<Integer>();
			
			treeAccess.seek(address);
			int keySize = treeAccess.readInt();
			int leaf = treeAccess.readInt();
			int parent = treeAccess.readInt();
			
			// 데이터 인풋
			for(int idx = 0; idx < keySize; idx++) {
				treeAccess.seek(address + (idx * 2 + 3) * 4);
				ptrs.add(treeAccess.readInt());
				treeAccess.seek(ptrs.get(idx));
				keys.add(treeAccess.readInt());
			}
			if(leaf == 0) {
				treeAccess.seek(address + (keySize * 2 + 3) * 4);
				ptrs.add(treeAccess.readInt());
			}
   			
			node = new Node(keys, ptrs, leaf, parent, address);
			
		} catch (IOException e) {
			
			System.out.println("This is findNode #330");
			System.out.println(e);
			
		}
   		
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
			int ptrsSize = ptrs.size();
			
			System.out.println("-----(keySize : "+ keysSize + ")" + ", (ptrsSize : "+ ptrsSize+")-----");
			for(int idx = 0; idx < keysSize; idx++) {
				nodeBuf.putInt(ptrs.get(idx));
				nodeBuf.putInt(keys.get(idx));
			}
			
			if(node.leaf == 0)
				nodeBuf.putInt(ptrs.get(keysSize));
			
			treeAccess.write(nodeValue);
			
		}
		catch(IOException e) {
			
			System.out.println("This is writeNode #361");
			System.out.println(e);
			
		}
		
		System.out.println("-----writeNode End-----");
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