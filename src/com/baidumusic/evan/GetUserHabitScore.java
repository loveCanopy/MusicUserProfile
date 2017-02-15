package com.baidumusic.evan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import jodd.util.StringUtil;
import udf.FindIpArea;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;

public class GetUserHabitScore {
	private final static String TAB = "\t";
	private final static int DAY_OF_MONTH = 30;
	private static class ParseMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			InputSplit inputSplit = (InputSplit) context.getInputSplit();
			String pathName = ((FileSplit) inputSplit).getPath().toString();
			String line = value.toString();
			if (pathName.contains("UserPlayRecord")) {
				/*
				 * PCweb 00001A7A159A6CA42EC05F7D9260E111 265046969
				 * {"2016-11-27 09:08:53":"2016-11-27 09:08:53\t119.181.90.54\t59936\t0\ttingbox\t128"
				 * ,"2016-11-27 09:11:23":"2016-11-27 09:11:23\t119.181.90.54\t210755\t0\ttingbox\t128"}
				 */

				String[] lParts = line.split(TAB);
				if (4 != lParts.length)
					return;

				String uniqId = lParts[1];
				String strPlay = lParts[3];
				if (strPlay.matches("^\\{.*:.*\\}$")) {
					JSONObject strJson = JSON.parseObject(strPlay);
					for (Entry<String, Object> s : strJson.entrySet()) {
						String getVal = s.getValue().toString();
						String[] arr = getVal.split(TAB);
						if (6 == arr.length) {
							String logDate = arr[0];
							String eventIp = arr[1];
							double time = Math.round(1.0*Math.round(Double.valueOf(arr[2]))/1000);
							String playTime = String.valueOf(time > 0 ? time : 1);
							context.write(new Text(uniqId), new Text(logDate + TAB + playTime + TAB + eventIp));
						}
					}
				}
			}  else if (pathName.contains("MergeFMDownoadVisitUGC")) {
				// 00001940ECC3DB8D168C922B11221EBD {"2016-11-14 19:09:04":"2016-11-14 19:09:04\t175.0.244.186\tfm\t0"}
				// no-search no-sug no-favor no-del no-song no-album
				// no-favor no-comment no,-gedan no-order
				String[] lParts = StringUtil.split(line, TAB);
				if (12 == lParts.length) {
					String uniqId = lParts[0];
					String visitStr = lParts[1];
					if (getVisitSong(visitStr).size() > 0) {
						List<String> resList = getVisitSong(visitStr);
						for (String res : resList) {
							context.write(new Text(uniqId), new Text(res));
						}
					}

					String searchStr = lParts[3];
					if (getVisitSong(searchStr).size() > 0) {
						List<String> resList = getVisitSong(searchStr);
						for (String res : resList) {
							context.write(new Text(uniqId), new Text(res));
						}
					}

					String sugStr = lParts[3];
					if (getVisitSong(sugStr).size() > 0) {
						List<String> resList = getVisitSong(sugStr);
						for (String res : resList) {
							context.write(new Text(uniqId), new Text(res));
						}
					}
				}
			}
		}
		
		private List<String> getVisitSong(String str) {
			List<String> aList = new ArrayList<String>();
			if (str.matches("^\\{.*:.*\\}$")) {
				JSONObject strJson = JSON.parseObject(str);
				for (Entry<String, Object> s : strJson.entrySet()) {
					String getVal = s.getValue().toString();
					String[] arr = getVal.split(TAB);
					if (4 == arr.length) {
						aList.add(arr[0] + TAB + arr[1]);
					}
				}
			}
			return aList;
		}
	}

	private static class ParseReduce extends Reducer<Text, Text, Text, Text> {
		private static FindIpArea area = new FindIpArea();
		private boolean isNull(String str) {
			if (str == null || str.length() == 0) {
				return true;
			}
			return false;
		}
		
		public static HashMap<String, Integer> AddMap(HashMap<String, Integer> map, String key) {
			if (map.containsKey(key)) {
				int cnt = map.get(key);
				map.put(key, cnt + 1);
			} else {
				map.put(key, 1);
			}
			return map;
		}
		
		public static HashMap<String, Long> AddMap2(HashMap<String, Long> map, String key, long val) {
			if (map.containsKey(key)) {
				long cnt = map.get(key);
				map.put(key, cnt + val);
			} else {
				map.put(key, val);
			}
			return map;
		}
		
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//logDate + TAB + playTime + TAB + eventIp + TAB + formatRate(rate)
			HashMap<String, String> timeMap = new HashMap<String, String>();
			HashMap<String, Integer> ipMap = new HashMap<String, Integer>();
			for (Text val : values) {
				String[] vParts = val.toString().split(TAB);
				if (3 == vParts.length) {
					String logDate = vParts[0];
					String playTime = vParts[1];
					timeMap.put(logDate, playTime);

					String ip = vParts[2];
					if (!isNull(ip) && ip.matches("[.0-9]+")) {
						String province = area.evaluate(new Object[] { ip, "a", "3" });
						String city = area.evaluate(new Object[] { ip, "a", "4" });
						if (!isNull(province) && !isNull(city) && !(province.equals("None") && city.equals("None"))
								&& !(province.equals("else:com.baidu.com") || city.equals("else:com.baidu.com"))) {
							String area = province + "/" + city;
							ipMap = AddMap(ipMap, area);
						}
					}
				} else if (2 == vParts.length) {
					String logDate = vParts[0];
					timeMap.put(logDate, "0");
					String ip = vParts[1];
					if (!isNull(ip) && ip.matches("[.0-9]+")) {
						String province = area.evaluate(new Object[] { ip, "a", "3" });
						String city = area.evaluate(new Object[] { ip, "a", "4" });
						if (!isNull(province) && !isNull(city) && !(province.equals("None") && city.equals("None"))
								&& !(province.equals("else:com.baidu.com") || city.equals("else:com.baidu.com"))) {
							String area = province + "/" + city;
							ipMap = AddMap(ipMap, area);
						}
					}
				}
			}
			
			StringBuilder sb = new StringBuilder();
			
			// ip
			int count = 0;
			String smaxIp = null;
			int imaxIp = 0;
			for (Entry<String, Integer> entry : ipMap.entrySet()) {
				count++;
				String ip = entry.getKey();
				int val = entry.getValue();
				if (val > imaxIp) {
					imaxIp = val;
					smaxIp = ip;
				}
			}
			long ipVal = (count == 0 ? 0 : Math.round(imaxIp * 1.0 / count));
			sb.append("所在地/").append(smaxIp).append("|").append(ipVal).append(",");
			
			// time
			long totalPlayTime = 0;
			int cnt = 0;
			HashMap<String, Long> hourMap = new HashMap<String, Long>();
			HashMap<String, Long> weekMap = new HashMap<String, Long>();
			HashMap<String, Integer> weekOfMonthMap = new HashMap<String, Integer>();
			HashSet<String> daySet = new HashSet<String>();
			for (Entry<String, String> entry : timeMap.entrySet()) {
				String logDate = entry.getKey();
				String playTime = entry.getValue();
				long iPT = Math.round(Double.valueOf(playTime));
				totalPlayTime += iPT;
				if (iPT > 0) {
					cnt++;

					//一日分布
					String hour = Tools.parseHour(logDate);
					hourMap = AddMap2(hourMap, hour, iPT);
					
					//一周分布
					String week = Tools.parseWeek(logDate);
					weekMap = AddMap2(weekMap, week, iPT);
				}
				
				// 周活跃
				String weekOfMonth = String.valueOf(Tools.parseWeekOfMonth(logDate));
				weekOfMonthMap = AddMap(weekOfMonthMap, weekOfMonth);
				
				// 日活跃
				String day = logDate.split("\\s")[0].replace("-", "");
				daySet.add(day);
			}
			
			for (Entry<String, Long> entry : hourMap.entrySet()) {
				String hour = entry.getKey();
				long playTime = entry.getValue();
				String time = String.valueOf(Math.round(1.0 * playTime / cnt)) + "s";
				long val = Math.round(100.0 * daySet.size() / DAY_OF_MONTH);
				String res = "听歌时间段/一日分布/" + hour + "/" + time + "|" + val;
				sb.append(res).append(",");
			}
			
			for (Entry<String, Long> entry : weekMap.entrySet()) {
				String week = entry.getKey();
				long playTime = entry.getValue();
				String time = String.valueOf(Math.round(1.0 * playTime / cnt / 60)) + "m";
				long val = Math.round(100.0 * daySet.size() / DAY_OF_MONTH);
				String res = "听歌时间段/一周分布/" + week + "/" + time + "|" + val;
				sb.append(res).append(",");
			}
			
			int weekCnt = 0;
			int weekSum = 0;
			for (Entry<String, Integer> entry : weekOfMonthMap.entrySet()) {
				weekCnt++;
				int days = entry.getValue();
				weekSum += days;
			}
			
			if (weekCnt > 0) {
				long val = Math.round(weekSum * 1.0 / weekCnt);
				long score = Math.round(weekCnt * 100.0 / 5);
				String res = "活跃度/周活跃/" + val + "|" + score;
				sb.append(res).append(",");
			}
			
			int dayCnt = daySet.size();
			if (dayCnt > 0) {
				String dayVal = Math.round(totalPlayTime * 1.0 / 1000 / 60 / dayCnt) + "s";
				long score = Math.round(dayCnt * 100.0 / DAY_OF_MONTH);
				String res = "活跃度/日活跃/" + dayVal + "|" + score;
				sb.append(res);
			}
			
			context.write(key, new Text(sb.toString()));
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, String input)
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		FileSystem hdfs = FileSystem.get(conf);
		String output = "/user/work/evan/output/GetUserHabitScore";
		hdfs.delete(new Path(output), true);

		Job job = Job.getInstance(conf);
		job.setJarByClass(GetUserHabitScore.class);
		job.setJobName("Evan_GetUserHabitScore");
		job.setNumReduceTasks(10);
		job.setMapperClass(ParseMapper.class);
		job.setReducerClass(ParseReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(output));
		return job.waitForCompletion(true);
	}

	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();

		if (args.length == 0) {
			System.err.println("Usage: class <in> <out> ");
			System.exit(1);
		}

		String queue = "mapred";
		if (args.length > 1) {
			queue = args[1].matches("hql|dstream|mapred|udw|user|common") ? args[1] : "mapred";
		}
		conf.set("mapreduce.job.queuename", queue);

		GetUserHabitScore.runLoadMapReducue(conf, args[0]);
	}
}