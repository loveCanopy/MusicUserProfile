package com.baidumusic.evan;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AnalyQukuSongs {
	private static final String logEntryPattern = "^(\\d+),(\\d+)\t(.*)";
	private static final String TAB = "\t";
	private static HashMap<String, String> hashMap = new HashMap<String, String>();
	private static BufferedReader bufferedReader;
	
	private static void getInitTag(String path, HashMap<String, String> hashMap) 
			throws IOException {
		String encoding = "UTF-8";
		File file = new File(path);
		if (file.isFile() && file.exists()) {
			InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);
			bufferedReader = new BufferedReader(read);
			String line = null;
			while ((line = bufferedReader.readLine()) != null) {
				String[] lparts = line.split(TAB);
				if (2 == lparts.length) {
					hashMap.put(lparts[0], lparts[1]);
				}
			}
			read.close();
		}
	}

	private static String getItems(String line, int idx) {
		Pattern p = Pattern.compile(logEntryPattern);
		Matcher matcher = p.matcher(line);
		if (!matcher.matches() || 4 != matcher.groupCount()) {
			int col = Integer.parseInt(matcher.group(1));
			int row = Integer.parseInt(matcher.group(2));
			int total_col = col * row;
			int start = 0;
			int end = 0;
			String other = matcher.group(3);
			String[] slist = other.split(TAB);
			int flag = 0;
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < slist.length; i++) {
				if (i == total_col) {
					flag++;
					String[] temp = slist[i].split(",");
					col = Integer.parseInt(temp[0]);
					row = Integer.parseInt(temp[1]);
					start = 0;
					total_col = total_col + 1 + col * row;
				} else {
					if (flag == idx) {
						slist[i] = slist[i].replaceAll("^^M+", "");
						slist[i] = slist[i].replaceAll("^M$+", "");
						sb.append(slist[i]);
						start++;
						if (start == col) {
							end++;
							if (end == row) {
								return sb.toString();
							} else {
								sb.append(";");
							}
							start = 0;
						} else {
							sb.append(TAB);
						}
					}
				}
			}
		}
		return "";
	}

	public static void readTxtFile(String path, String filePath, String fileOut) throws IOException {
		getInitTag(path, hashMap);
		try {
			String encoding = "UTF-8";
			File file = new File(filePath);
			if (file.isFile() && file.exists()) {
				InputStreamReader read = new InputStreamReader(new FileInputStream(file), encoding);
				BufferedReader bufferedReader = new BufferedReader(read);

				File file2write = new File(fileOut);
	 
				// if file doesnt exists, then create it
				if (!file2write.exists()) {
					file2write.createNewFile();
				}
	 
				FileWriter fw = new FileWriter(file2write.getAbsoluteFile());
				BufferedWriter bw = new BufferedWriter(fw);
				
				String line = null;
				int cnt = 0;
				while ((line = bufferedReader.readLine()) != null) {
					if (line.matches("^\\d+,\\d+\t.*")) {
						String basicInfo = getItems(line, 0);
						String[] basicPart = basicInfo.split(TAB);
						if (basicPart.length < 85) continue;
						String singerId = basicPart[0];
						String singerName = basicPart[1];
						String songId = basicPart[2];
						String songName = basicPart[3];
						String style = basicPart[8].length() > 0 ? basicPart[8] : "null";
						String language = basicPart[18].length() > 0 ? basicPart[18] : "null";
						String publishTime = basicPart[19].length() > 0 ? basicPart[19] : "0000-00-00";
						
						String yinpinInfo = getItems(line, 1);
						String rate = "";
						if (yinpinInfo.length() > 0) {
							String[] ratePart = yinpinInfo.split(";");
							for (String m : ratePart) {
								String[] lpart = m.split(TAB);
								if (18 == lpart.length) {
									rate = lpart[1] + TAB + lpart[4];
									System.out.println(rate);
									System.out.println(m);
								}
							}
						}
						
						
						String markInfo = getItems(line, 10);
						HashSet<String> markSet = new HashSet<String>();
						if (!style.equals("null")) {
							markSet.add(style);
						}
						String mark = "";
						if (markInfo.length() > 0) {
							String[] markpart = markInfo.split(";");
							for (String m : markpart) {
								String[] lpart = m.split(TAB);
								if (5 == lpart.length) {
									mark = lpart[4].replaceAll(",", "");
									markSet.add(mark);
									
									String tiTagid = lpart[1];
									if (hashMap.containsKey(tiTagid)) {
										markSet.add(hashMap.get(tiTagid));
									}
								}
							}
						}
						String res = markSet.toString().replaceAll("\\[|\\]|\\s+", "");
						String result = singerId 
								+ TAB + singerName
								+ TAB + songId
								+ TAB + songName
								+ TAB + language
								+ TAB + publishTime
								+ TAB + (res.length() > 0 ? res : "null");
						System.out.println(++cnt + "\t" + result.replaceAll("\\|", ""));
						bw.write(result.replaceAll("\\|", ""));
						bw.write("\n");
					}
				}
				read.close();
				bw.close();
				System.out.println("Done");
			} else {
				System.out.println("File not exist..");
			}
		} catch (Exception e) {
			System.out.println("Error reading file..");
			e.printStackTrace();
		}
		System.out.println("hashmap.size() = " + hashMap.size());
	}

	public static void main(String args[]) throws IOException {
		//if (args.length < 2) {
		//	System.err.println("Usage: Class <path> <infile> <outfile>");
		//	System.exit(-1);
		//}
		String path = "C:\\Users\\Administrator\\Desktop\\quku_tag_dict_out.txt";
		String filePath = "C:\\Users\\Administrator\\Desktop\\song.txt";
		String fileOut = "C:\\Users\\Administrator\\Desktop\\song_result.txt";
		
		//String path = args[0];
		//String filePath = args[1];
        //String fileOut = args[2];

		readTxtFile(path, filePath, fileOut);
	}
}
