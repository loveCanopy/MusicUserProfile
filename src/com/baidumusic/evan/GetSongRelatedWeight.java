package com.baidumusic.evan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
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
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Map.Entry;
import java.util.regex.Pattern;

public class GetSongRelatedWeight {
	private static final String TAB = "\t";
	private static final Pattern COLON = Pattern.compile(":");
	private static DecimalFormat df = new DecimalFormat("0.000000");

	private static class MergeMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			InputSplit inputSplit = (InputSplit) context.getInputSplit();
			String pathName = ((FileSplit) inputSplit).getPath().toString();
			String line = value.toString();
			if (pathName.contains("UserPlayRecord")) {
				// PCweb 00001309DE884830F56AA93CFD63CEE6 490468
				// {"2016-11-3011:48:40":"2016-11-30
				// 11:48:40\t222.129.185.146\t6246\t0\ttingbox\t128"}
				/*
				 * device_type + TAB + user_uniq_id + TAB + song_id logDate +
				 * TAB + event_ip + TAB + play_time + TAB + favor + TAB + ref +
				 * TAB + rate
				 */
				String[] lParts = StringUtil.split(line, TAB);
				if (4 == lParts.length) {
					String uniqId = lParts[1];
					String songId = lParts[2];
					String itemsStr = lParts[3];
					int iPlay = 0;
					int iFavor = 0;
					String rate = "000";
					if (itemsStr.matches("^\\{.*:.*\\}$")) {
						JSONObject itemsJson = JSON.parseObject(itemsStr);
						for (Entry<String, Object> s : itemsJson.entrySet()) {
							iPlay++;
							String getVal = s.getValue().toString();
							String[] arr = getVal.split(TAB);
							if (6 != arr.length)
								continue;
							iFavor += Integer.parseInt(arr[3]);
							rate = Tools.formatRate(arr[5]);
						}
					}
					context.write(new Text(uniqId + TAB + songId), new Text("bofang" + TAB + iPlay + TAB + rate));
					context.write(new Text(uniqId + TAB + songId), new Text("shoucang" + TAB + iFavor));
				}
			} else if (pathName.contains("MergeFMDownoadVisitUGC")) {
				// 00001940ECC3DB8D168C922B11221EBD {"2016-11-14
				// 19:09:04":"2016-11-14 19:09:04\t175.0.244.186\tfm\t0"}
				// no-search no-sug no-favor no-del no-song no-album
				// no-favor no-comment no-gedan no-order
				String[] lParts = StringUtil.split(line, TAB);
				if (12 == lParts.length) {
					String uniqId = lParts[0];
					String visitStr = lParts[1];
					if (getVisitSong(visitStr).size() > 0) {
						List<String> resList = getVisitSong(visitStr);
						for (String songId : resList) {
							context.write(new Text(uniqId + TAB + songId), new Text("fangwen" + TAB + "1"));
						}
					}

					String sugStr = lParts[3];
					if (getVisitSong(sugStr).size() > 0) {
						List<String> resList = getVisitSong(sugStr);
						for (String songId : resList) {
							context.write(new Text(uniqId + TAB + songId), new Text("sug" + TAB + "1"));
						}
					}

					String fmFavorStr = lParts[4];
					if (getDownSong(fmFavorStr).size() > 0) {
						List<String> resList = getDownSong(fmFavorStr);
						for (String songId : resList) {
							context.write(new Text(uniqId + TAB + songId), new Text("fmfavor" + TAB + "1"));
						}
					}

					String fmDelStr = lParts[5];
					if (getDownSong(fmDelStr).size() > 0) {
						List<String> resList = getDownSong(fmDelStr);
						for (String songId : resList) {
							context.write(new Text(uniqId + TAB + songId), new Text("fmdel" + TAB + "1"));
						}
					}

					String downSongStr = lParts[6];
					if (getDownSong(downSongStr).size() > 0) {
						List<String> resList = getDownSong(downSongStr);
						for (String songId : resList) {
							context.write(new Text(uniqId + TAB + songId), new Text("xiazai" + TAB + "1"));
						}
					}

					String gedanStr = lParts[10];
					if (getGDSong(gedanStr).size() > 0) {
						List<String> resList = getGDSong(gedanStr);
						for (String songId : resList) {
							context.write(new Text(uniqId + TAB + songId), new Text("gedan" + TAB + "1"));
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
					if (4 == arr.length && arr[2].equals("song")) {
						aList.add(arr[3]);
					}
				}
			}
			return aList;
		}

		private List<String> getDownSong(String str) {
			List<String> aList = new ArrayList<String>();
			if (str.matches("^\\{.*:.*\\}$")) {
				JSONObject strJson = JSON.parseObject(str);
				for (Entry<String, Object> s : strJson.entrySet()) {
					String getVal = s.getValue().toString();
					String[] arr = getVal.split(TAB);
					if (3 == arr.length) {
						aList.add(arr[2]);
					}
				}
			}
			return aList;
		}

		private List<String> getGDSong(String str) {
			List<String> aList = new ArrayList<String>();
			if (str.matches("^\\{.*:.*\\}$")) {
				JSONObject strJson = JSON.parseObject(str);
				for (Entry<String, Object> s : strJson.entrySet()) {
					String getVal = s.getValue().toString();
					String[] arr = getVal.split(TAB);
					if (4 == arr.length) {
						String[] songIds = arr[3].split(",");
						for (String song : songIds) {
							aList.add(song);
						}
					}
				}
			}
			return aList;
		}
	}

	private static class MergeReduce extends Reducer<Text, Text, Text, Text> {
		private HashMap<String, Integer> AddMap(HashMap<String, Integer> map, String line) {
			String[] lParts = line.split(TAB);
			if (2 == lParts.length) {
				if (map.containsKey(lParts[0])) {
					int cnt = map.get(lParts[0]);
					map.put(lParts[0], cnt + Integer.parseInt(lParts[1]));
				} else {
					map.put(lParts[0], Integer.parseInt(lParts[1]));
				}
			}
			return map;
		}

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
			List<String> rateList = new ArrayList<String>();
			rateList.add("000");
			for (Text val : values) {
				String valStr = val.toString();
				String[] valPart = valStr.split(TAB);
				if (valPart.length == 3) {
					rateList.add(valPart[2]);
					String tmp = valPart[0] + TAB + valPart[1];
					hashMap = AddMap(hashMap, tmp);
				} else {
					hashMap = AddMap(hashMap, val.toString());
				}
			}
			int bofang = hashMap.containsKey("bofang") ? hashMap.get("bofang") : 0;
			int shoucang = hashMap.containsKey("shoucang") ? hashMap.get("shoucang") : 0;
			int fangwen = hashMap.containsKey("fangwen") ? hashMap.get("fangwen") : 0;
			int sug = hashMap.containsKey("sug") ? hashMap.get("sug") : 0;
			int fmfavor = hashMap.containsKey("fmfavor") ? hashMap.get("fmfavor") : 0;
			int fmdel = hashMap.containsKey("fmdel") ? hashMap.get("fmdel") : 0;
			int xiazai = hashMap.containsKey("xiazai") ? hashMap.get("xiazai") : 0;
			int gedan = hashMap.containsKey("gedan") ? hashMap.get("gedan") : 0;
			String res = rateList.toString().replaceAll(" ", "");
			context.write(new Text(key.toString() + TAB + res.substring(1, res.length() - 1)), new Text(bofang + TAB
					+ shoucang + TAB + fangwen + TAB + sug + TAB + fmfavor + TAB + fmdel + TAB + xiazai + TAB + gedan));
		}
	}

	private static long atol(String s) {
		return Long.valueOf(s).longValue();
	}

	private static class GetMinMaxMap extends Mapper<LongWritable, Text, IntWritable, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] lparts = line.split(TAB);
			int len = lparts.length;
			/*
			 * 935869120 490468 0 0 0 0 0 0 1 0
			 */
			if (len < 4)
				return;
			StringBuilder sb = new StringBuilder();
			for (int i = 3; i < len; i++) {
				sb.append(lparts[i]).append(":").append(lparts[i]).append(TAB);
			}
			context.write(new IntWritable(len - 3), new Text(sb.toString()));
		}
	}

	private static class GetMinMaxReduce extends Reducer<IntWritable, Text, IntWritable, Text> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int length = key.get();
			String[] strArr = new String[length];
			for (int i = 0; i < length; i++) {
				strArr[i] = Integer.MAX_VALUE + ":" + 0;
			}

			for (Text val : values) {
				String line = val.toString();
				StringTokenizer st = new StringTokenizer(line, " \t\n");
				int idx = 0;
				while (st.hasMoreTokens()) {
					String[] arrsplit = COLON.split(strArr[idx++]);
					long curMin = atol(arrsplit[0]);
					long curMax = atol(arrsplit[1]);
					String[] split = COLON.split(st.nextToken(), 2);
					if (split == null || split.length < 2) {
						throw new RuntimeException("Wrong input format at line " + line);
					}

					try {
						long min = atol(split[0]);
						long max = atol(split[1]);
						if (min < curMin) {
							curMin = min;
						}

						if (max > curMax) {
							curMax = max;
						}

						strArr[idx - 1] = curMin + ":" + curMax;
					} catch (NumberFormatException e) {
						throw new RuntimeException("Wrong input format at line " + line, e);
					}
				}
			}

			StringBuilder sb = new StringBuilder();
			sb.append(strArr[0]);
			for (int i = 1; i < length; i++) {
				sb.append(TAB).append(strArr[i]);
			}
			context.write(key, new Text(sb.toString()));
		}
	}

	private static class ScaleMap extends Mapper<LongWritable, Text, Text, NullWritable> {
		private static String[] strArr;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			File file = new File("min.max");
			BufferedReader br = new BufferedReader(new FileReader(file));

			String line = "";
			while ((line = br.readLine()) != null) {
				String[] lpart = line.split(TAB);
				int length = Integer.valueOf(lpart[0]);
				strArr = new String[length];
				for (int i = 1; i < lpart.length; i++) {
					strArr[i - 1] = lpart[i];
				}
			}
			br.close();
			System.out.println("strArr.length = " + strArr.length);
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] lparts = line.split(TAB);
			int len = lparts.length;
			if (len < 4)
				return;
			String uniqId = lparts[0];
			String songID = lparts[1];
			String rate = lparts[2];
			StringBuilder sb = new StringBuilder();
			sb.append(uniqId).append(TAB).append(songID).append(TAB).append(rate).append(TAB);
			for (int i = 3; i < len; i++) {
				long feature = atol(lparts[i]);
				long min = atol(strArr[i - 3].split(":")[0]);
				long max = atol(strArr[i - 3].split(":")[1]);
				String newval = getWeight(feature, min, max);
				if (i == len - 1) {
					sb.append(newval);
				} else {
					sb.append(newval).append("|");
				}
			}
			context.write(new Text(sb.toString()), NullWritable.get());
		}
		
		private String getWeight(long val, long min, long max) {
			double normal = ((max - min == 0) ? 0.0 : 1.0 * (val - min) / (max - min));
			if (max < 10) {
				return df.format(normal);
			} else if (max < 2000) {
				if (val < 1) {
					return df.format(normal);
				} else if (val < 10) {
					normal = 0.5 + 0.1 * normal;
				} else if (val < 20) {
					normal = 0.6 + 0.1 * normal;
				} else if (val < 30) {
					normal = 0.7 + 0.1 * normal;
				} else if (val < 40) {
					normal = 0.8 + 0.1 * normal;
				} else {
					normal = 0.9 + 0.1 * normal;
				}
			} else {
				if (val < 1) {
					return df.format(normal);
				} else if (val < 5) {
					normal = 0.3 + 0.1 * normal;
				} else if (val < 20) {
					normal = 0.5 + 0.1 * normal;
				} else if (val < 50) {
					normal = 0.6 + 0.1 * normal;
				} else if (val < 60) {
					normal = 0.7 + 0.1 * normal;
				} else if (val < 100) {
					normal = 0.8 + 0.1 * normal;
				} else {
					normal = 0.9 + 0.1 * normal;
				}
			}
			return df.format(normal);
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, String input)
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Path outPath1 = new Path("/user/work/evan/tmp/GetSongRelatedWeight1");
		Path outPath2 = new Path("/user/work/evan/tmp/GetSongRelatedWeight2");
		Path outPath3 = new Path("/user/work/evan/tmp/GetSongRelatedWeight3");

		FileSystem hdfs = FileSystem.get(conf);
		Job job = Job.getInstance(conf);
		job.setJarByClass(GetSongRelatedWeight.class);
		job.setJobName("Evan_GetSongRelatedWeight-L1");
		job.setNumReduceTasks(100);
		job.setMapperClass(MergeMapper.class);
		job.setReducerClass(MergeReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, outPath1);
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		hdfs.delete(outPath1, true);
		job.waitForCompletion(true);

		job = Job.getInstance(conf, "Evan_GetSongRelatedWeight-L2");
		job.setJarByClass(GetSongRelatedWeight.class);
		job.setMapperClass(GetMinMaxMap.class);
		job.setCombinerClass(GetMinMaxReduce.class);
		job.setReducerClass(GetMinMaxReduce.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, outPath1);
		FileOutputFormat.setOutputPath(job, outPath2);
		job.setNumReduceTasks(1);
		hdfs.delete(outPath2, true);
		job.waitForCompletion(true);

		job = Job.getInstance(conf, "Evan_GetSongRelatedWeight-L3");
		job.setJarByClass(GetSongRelatedWeight.class);
		job.setMapperClass(ScaleMap.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.addCacheFile(new URI(outPath2 + "/part-r-00000#min.max"));
		FileInputFormat.setInputPaths(job, outPath1);
		FileOutputFormat.setOutputPath(job, outPath3);
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		job.setNumReduceTasks(0);
		hdfs.delete(outPath3, true);
		return job.waitForCompletion(true);
	}

	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();

		if (args.length == 0) {
			System.err.println("Usage: class <in>");
			System.exit(1);
		}

		String queue = "mapred";
		if (args.length > 1) {
			queue = args[1].matches("hql|dstream|mapred|udw|user|common") ? args[1] : "mapred";
		}
		conf.set("mapreduce.job.queuename", queue);

		GetSongRelatedWeight.runLoadMapReducue(conf, args[0]);
	}
}