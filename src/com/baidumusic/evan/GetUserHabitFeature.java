package com.baidumusic.evan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import udf.FindIpArea;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;

public class GetUserHabitFeature {
	private final static String TAB = "\t";
	private static final DateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static Calendar cal = Calendar.getInstance();	
	private static class ParseMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			/*
			 * PCweb 00001A7A159A6CA42EC05F7D9260E111 265046969
			 * {"2016-11-27 09:08:53"
			 * :"2016-11-27 09:08:53\t119.181.90.54\t59936\t0\ttingbox\t128"
			 * ,"2016-11-27 09:11:23"
			 * :"2016-11-27 09:11:23\t119.181.90.54\t210755\t0\ttingbox\t128"}
			 */

			String line = value.toString();
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
						String playTime = arr[2];
						String rate = arr[5];
						
						context.write(new Text(uniqId),
								new Text(logDate + TAB + playTime + TAB + eventIp + TAB + formatRate(rate)));
					}
				}
			}
		}
		
		public static String formatRate(String rate) {
			String res = "000";
			if (!rate.matches("[0-9]+")) {
				return res;
			}
			int irate = Integer.valueOf(rate);
			if (irate <= 0) {
				return res;
			}
			if (irate < 32 || irate < (32 + 64) / 2) {
				res = "32";
			} else if (irate < 64 || irate < (64 + 96) / 2) {
				res = "64";
			} else if (irate < 96 || irate < (96 + 128) / 2) {
				res = "96";
			} else if (irate < 128 || irate < (128 + 256) / 2) {
				res = "128";
			} else if (irate < 256 || irate < (256 + 320) / 2) {
				res = "256";
			} else if (irate < 320 || irate < (320 + 500) / 2) {
				res = "320";
			} else {
				res = "320plus";
			}

			return res;
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
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//logDate + TAB + playTime + TAB + eventIp + TAB + formatRate(rate)
			HashMap<String, String> timeMap = new HashMap<String, String>();
			HashMap<String, Integer> ipMap = new HashMap<String, Integer>();
			HashMap<String, Integer> rateMap = new HashMap<String, Integer>();
			for (Text val : values) {
				String[] vParts = val.toString().split(TAB);
				if (4 != vParts.length)
					continue;
				
				String logDate = vParts[0];
				String playTime = vParts[1];
				timeMap.put(logDate, playTime);
				
				String ip = vParts[2];
				if (!isNull(ip) && ip.matches("[.0-9]+")) {
					String province = area.evaluate(new Object[] {ip, "a", "3"});
					String city = area.evaluate(new Object[] {ip, "a", "4"});
					if (!isNull(province) && !isNull(city) 
							&& !(province.equals("None") && city.equals("None"))
							&& !(province.equals("else:com.baidu.com") || city.equals("else:com.baidu.com"))) {
						String area = province + "/" + city;
						ipMap = AddMap(ipMap, area);
					}
				}
				
				String rate = vParts[3];
				if (!rate.equals("000")) {
					rateMap = AddMap(rateMap, rate);
				}
			}
			
			// ip
			for (Entry<String, Integer> entry : ipMap.entrySet()) {
				String ip = entry.getKey();
				int val = entry.getValue();
				context.write(key, new Text(ip + TAB + val));
			}
			
			// rate
			for (Entry<String, Integer> entry : rateMap.entrySet()) {
				String rate = entry.getKey();
				int val = entry.getValue();
				context.write(key, new Text(rate + TAB + val));
			}

			// time
			for (Entry<String, String> entry : timeMap.entrySet()) {
				String logDate = entry.getKey();
				String playTime = entry.getValue();

				int[] hourArr = new int[24];
				int[] dayArr = new int[31];
				int[] weekArr = new int[7];

				Date d = new Date();
				try {
					d = dateformat.parse(logDate);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				cal.setTime(d);
				int hour = cal.get(Calendar.HOUR_OF_DAY);
				int week = cal.get(Calendar.DAY_OF_WEEK) - 1;
				int day = cal.get(Calendar.DAY_OF_MONTH) - 1;
				
			}
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, String input)
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		FileSystem hdfs = FileSystem.get(conf);
		String output = "/user/work/evan/output/GetUserHabitFeature";
		hdfs.delete(new Path(output), true);

		Job job = Job.getInstance(conf);
		job.setJarByClass(GetUserHabitFeature.class);
		job.setJobName("Evan_GetUserHabitFeature");
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

		GetUserHabitFeature.runLoadMapReducue(conf, args[0]);
	}
}