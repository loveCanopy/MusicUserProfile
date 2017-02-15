package com.baidumusic.evan;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.StringUtils;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

public class ParseTagDict {
	private static final String TAB = "\t";
	private static HashMap<String, Node> hashMap = new HashMap<String, Node>();

	public static void readTxtFile(String filePath, String fileOut) {
		try {
			String encoding = "UTF-8";
			File file = new File(filePath);
			if (file.isFile() && file.exists()) {
				InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);
				BufferedReader bufferedReader = new BufferedReader(read);

				String line = null;
				while ((line = bufferedReader.readLine()) != null) {
					if (line.matches("^[0-9]{1,}\t.*")) {
						String[] lparts = line.split(TAB);
						if (5 != lparts.length)
							continue;
						String tag_id = lparts[0];
						String tag_name = lparts[1];
						// String tag_level = lparts[2];
						String tag_parentid = lparts[3];
						// String tag_category = lparts[4];

						Node pnode = createIfNotInMap(hashMap, tag_parentid, "", tag_parentid);
						List<Node> tmp = pnode.getChildList();
						Node cNode = createIfNotInMap(hashMap, tag_id, tag_name, tag_parentid);
						tmp.add(cNode);

						
					}
				}
				read.close();
				System.out.println("Done");
			} else {
				System.out.println("File not exist..");
			}
		} catch (Exception e) {
			System.out.println("Error reading file..");
			e.printStackTrace();
		}
	}

	private static Node createIfNotInMap(HashMap<String, Node> hashMap, String id, String name, String pId) {
		Node node = null;

		if (hashMap.containsKey(id)) {
			node = hashMap.get(id);
			if (StringUtils.isNotBlank(name)) {
				node.setName(name);
				node.setParentId(pId);
			}

		} else {
			node = new Node(pId);
			node.setText(id);
			node.setName(name);
			hashMap.put(id, node);
		}
		return node;
	}

	static void outTree0(Node node, StringBuilder sb) {
		if (node == null) {
			return;
		}
		if (StringUtils.equals(node.getText(), node.getParentId())) {
			sb.append(node.getText());
			if (StringUtils.isNotBlank(node.getName())) {
				sb.append("(").append(node.getName()).append(")");
			}
			sb.append("->");
			return;
		}
		Node newNode = hashMap.get(node.getParentId());
		outTree0(newNode, sb);
		sb.append(node.getText());
		if (StringUtils.isNotBlank(node.getName())) {
			sb.append("(").append(node.getName()).append(")");
		}
		sb.append("->");
	}
	
	static void outTree(Node node, StringBuilder sbName, StringBuilder sbId) {
		if (node == null) {
			return;
		}
		if (StringUtils.equals(node.getText(), node.getParentId())) {
			sbId.append(node.getText());
			sbId.append("/");
			if (StringUtils.isNotBlank(node.getName())) {
				sbName.append(node.getName());
			}
			sbName.append("/");
			return;
		}
		Node newNode = hashMap.get(node.getParentId());
		outTree(newNode, sbName, sbId);
		sbId.append(node.getText());
		if (StringUtils.isNotBlank(node.getName())) {
			sbName.append(node.getName());
		}
		sbName.append("/");
		sbId.append("/");
	}
	
	public static void main(String args[]) throws IOException {
		String filePath = "C:\\Users\\Administrator\\Desktop\\quku_tag_dict.txt";
		String fileOut = "C:\\Users\\Administrator\\Desktop\\quku_tag_dict_out.txt";
		
		readTxtFile(filePath, fileOut);

		List<String> testList = new ArrayList<>(hashMap.size());
		testList.addAll(hashMap.keySet());
		
		File file2write = new File(fileOut);
		// if file doesnt exists, then create it
		if (!file2write.exists()) {
			file2write.createNewFile();
		}

		FileWriter fw = new FileWriter(file2write.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
	    Comparator<String> comparator = new Comparator<String>() {
	        public int compare(String keyA, String keyB) {
	        	long valA = NumberUtils.toLong(keyA);
				long valB = NumberUtils.toLong(keyB);
				if (valA >= valB) {
					return 1;
				} else {
					return -1;
				}
	        }
	    };
		Collections.sort(testList, comparator);
		for (String id : testList) {
			Node newNode = hashMap.get(id);
			StringBuilder sbName = new StringBuilder();
			StringBuilder sbId = new StringBuilder();
			outTree(newNode, sbName, sbId);
			String resName = sbName.subSequence(0, sbName.length() - 1).toString();
			if (resName == null || resName.length() < 1)
				continue;
			String resId = sbId.subSequence(0, sbId.length() - 1).toString();
			String[] idPart = resId.split("/");
			System.out.println(Integer.parseInt(idPart[idPart.length-1]) + "\t" + resName.substring(1));
			bw.write(Integer.parseInt(idPart[idPart.length-1]) + "\t" + resName.substring(1));
			bw.write("\n");
		}
		bw.close();
	}
}
