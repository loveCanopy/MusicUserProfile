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
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map.Entry;

public class GetSongRelatedScore {
	private final static String TAB = "\t";
	private static DecimalFormat df = new DecimalFormat("0.000000");

	private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			/*
			 * 867117577 1002995
			 * 000,128
			 * 0.300001|0.000000|0.000000|0.000000|0.000000|0.000000|0.000000|0.000000
			 * 中岛美嘉|日语|2006-12-13|日本流行,插曲,粤语,好听,原型朋克,摇滚/原型朋克,流行,心情,娜娜电影版2插曲,动漫,
			 * 娜娜电影版2插曲,影视原,流行/日本流行,合辑,none,乐队,欧美,偶像,舞曲
			 */
			String line = value.toString();
			String[] lParts = line.split(TAB);
			if (5 == lParts.length) {
				String uniqId = lParts[0];
				String rate = lParts[2];
				String weight = lParts[3];
				String tag = lParts[4];
				context.write(new Text(uniqId), new Text(weight + TAB + tag + TAB + rate));
			}
		}
	}

	private static class MyReduce extends Reducer<Text, Text, Text, Text> {
		private Double atof(String str) {
			return Double.valueOf(str).doubleValue();
		}

		private String strAddstr(String str1, String str2) {
			if (str1.length() == 0) {
				return "1:" + str2;
			}
			if (str2.length() == 0) {
				return "1:" + str1;
			}

			double[] res = new double[8];
			String[] sPart1 = str1.split(":")[1].split("\\|");
			String[] sPart2 = str2.split(":")[1].split("\\|");
			int cnt = Integer.parseInt(str1.split(":")[0]) + Integer.parseInt(str2.split(":")[0]);
			for (int i = 0; i < res.length; i++) {
				res[i] = atof(sPart1[i]) + atof(sPart2[i]);
			}
			StringBuilder sb = new StringBuilder();
			sb.append(cnt).append(":");
			for (int i = 0; i < res.length; i++) {
				if (i == 0) {
					sb.append(df.format(res[i]));
				} else {
					sb.append("|").append(df.format(res[i]));
				}
			}
			return sb.toString();
		}

		private HashMap<String, String> AddMap(HashMap<String, String> map, String key, String val) {
			if (map.containsKey(key)) {
				String tmp = map.get(key);
				map.put(key, strAddstr(tmp, val));
			} else {
				map.put(key, val);
			}
			return map;
		}

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String weight = "0:0|0|0|0|0|0|0|0";
			// 中岛美嘉|日语|2006-12-13|日本流行,插曲,粤语
			HashMap<String, String> singerMap = new HashMap<String, String>();
			HashMap<String, String> langMap = new HashMap<String, String>();
			HashMap<String, String> timeMap = new HashMap<String, String>();
			HashMap<String, String> styleMap = new HashMap<String, String>();
			HashMap<String, String> rateMap = new HashMap<String, String>();
			for (Text t : values) {
				String[] vpart = t.toString().split(TAB);
				String tmpWeight = "1:" + vpart[0];
				weight = strAddstr(weight, tmpWeight);
				String tag = vpart[1];
				String[] tagPart = tag.split("\\|");
				if (4 == tagPart.length) {
					if (!tagPart[0].equals("null")) {
						String singer = tagPart[0];
						String[] singerPart = singer.split(",");
						for (String s : singerPart) {
							String singerName = "歌手/" + s;
							singerMap = AddMap(singerMap, singerName, weight);
						}
					}

					if (!tagPart[1].equals("null")) {
						String language = "语言/" + tagPart[1];
						langMap = AddMap(langMap, language, weight);
					}

					if (!Tools.formatPublishtime(tagPart[2]).equals("null")) {
						String publishTime = "年代/" + Tools.formatPublishtime(tagPart[2]);
						timeMap = AddMap(timeMap, publishTime, weight);
					}

					if (!tagPart[3].equals("null")) {
						String style = tagPart[3];
						String[] stylePart = style.split(",");
						for (String s : stylePart) {
							styleMap = AddMap(styleMap, "风格/" + s, weight);
						}
					}

				}
				
				String rate = vpart[2];
				String[] ratePart = rate.split(",");
				if (0 == ratePart.length)
					continue;
				for (String r : ratePart) {
					if (!r.equals("000")) {
						rateMap = AddMap(rateMap, "音质偏好/" + r, weight);
					}
				}
			}

			// bofang shoucang fangwen sug fmfavor fmdel xiazai gedan
			// bfrz scgd fwrz sugjs dtxh dtsc xzgq tjdgd
			StringBuilder sb = new StringBuilder();

			// singer
			for (Entry<String, String> entry : singerMap.entrySet()) {
				String singerName = entry.getKey();
				String val = entry.getValue(); // 0:0|0|0|0|0|0|0|0
				int cnt = Integer.parseInt(val.split(":")[0]);
				String[] items = val.split(":")[1].split("\\|");
				double score = 0.0;
				if (items.length == 8) {
					score = (2 * Math.atan(cnt) / Math.PI) * (atof(items[0]) * Tools.itemSet.get("bfrz")
							+ atof(items[1]) * Tools.itemSet.get("scgd") + atof(items[2]) * Tools.itemSet.get("fwrz")
							+ atof(items[3]) * Tools.itemSet.get("sugjs") + atof(items[4]) * Tools.itemSet.get("dtxh")
							+ atof(items[5]) * Tools.itemSet.get("dtsc") + atof(items[6]) * Tools.itemSet.get("xzgq")
							+ atof(items[7]) * Tools.itemSet.get("tjdgd"));
				}
				sb.append(singerName).append("|").append(Math.round(score) > 100 ? 99 : Math.round(score)).append(",");
			}

			// language
			for (Entry<String, String> entry : langMap.entrySet()) {
				String lang = entry.getKey();
				String val = entry.getValue(); // 0:0|0|0|0|0|0|0|0
				int cnt = Integer.parseInt(val.split(":")[0]);
				String[] items = val.split(":")[1].split("\\|");
				double score = 0.0;
				if (items.length == 8) {
					score = (2 * Math.atan(cnt) / Math.PI) * (atof(items[0]) * Tools.itemSet.get("bfrz")
							+ atof(items[1]) * Tools.itemSet.get("scgd") + atof(items[2]) * Tools.itemSet.get("fwrz")
							+ atof(items[3]) * Tools.itemSet.get("sugjs") + atof(items[4]) * Tools.itemSet.get("dtxh")
							+ atof(items[5]) * Tools.itemSet.get("dtsc") + atof(items[6]) * Tools.itemSet.get("xzgq")
							+ atof(items[7]) * Tools.itemSet.get("tjdgd"));
				}
				sb.append(lang).append("|").append(Math.round(score) > 100 ? 99 : Math.round(score)).append(",");
			}

			// publish time
			for (Entry<String, String> entry : timeMap.entrySet()) {
				String time = entry.getKey();
				String val = entry.getValue(); // 0:0|0|0|0|0|0|0|0
				int cnt = Integer.parseInt(val.split(":")[0]);
				String[] items = val.split(":")[1].split("\\|");
				double score = 0.0;
				if (items.length == 8) {
					score = (2 * Math.atan(cnt) / Math.PI) * (atof(items[0]) * Tools.itemSet.get("bfrz")
							+ atof(items[1]) * Tools.itemSet.get("scgd") + atof(items[2]) * Tools.itemSet.get("fwrz")
							+ atof(items[3]) * Tools.itemSet.get("sugjs") + atof(items[4]) * Tools.itemSet.get("dtxh")
							+ atof(items[5]) * Tools.itemSet.get("dtsc") + atof(items[6]) * Tools.itemSet.get("xzgq")
							+ atof(items[7]) * Tools.itemSet.get("tjdgd"));
				}
				sb.append(time).append("|").append(Math.round(score) > 100 ? 99 : Math.round(score)).append(",");
			}

			// style
			for (Entry<String, String> entry : styleMap.entrySet()) {
				String style = entry.getKey();
				String val = entry.getValue(); // 0:0|0|0|0|0|0|0|0
				int cnt = Integer.parseInt(val.split(":")[0]);
				String[] items = val.split(":")[1].split("\\|");
				double score = 0.0;
				if (items.length == 8) {
					score = (2 * Math.atan(cnt) / Math.PI) * (atof(items[0]) * Tools.itemSet.get("bfrz")
							+ atof(items[1]) * Tools.itemSet.get("scgd") + atof(items[2]) * Tools.itemSet.get("fwrz")
							+ atof(items[3]) * Tools.itemSet.get("sugjs") + atof(items[4]) * Tools.itemSet.get("dtxh")
							+ atof(items[5]) * Tools.itemSet.get("dtsc") + atof(items[6]) * Tools.itemSet.get("xzgq")
							+ atof(items[7]) * Tools.itemSet.get("tjdgd"));
				}
				sb.append(style).append("|").append(Math.round(score) > 100 ? 99 : Math.round(score)).append(",");
			}
			
			// rate
			for (Entry<String, String> entry : rateMap.entrySet()) {
				String rate = entry.getKey();
				String val = entry.getValue(); // 0:0|0|0|0|0|0|0|0
				int cnt = Integer.parseInt(val.split(":")[0]);
				String[] items = val.split(":")[1].split("\\|");
				double score = 0.0;
				if (items.length == 8) {
					score = (2 * Math.atan(cnt) / Math.PI) * (atof(items[0]) * Tools.itemSet.get("bfrz")
							+ atof(items[1]) * Tools.itemSet.get("scgd") + atof(items[2]) * Tools.itemSet.get("fwrz")
							+ atof(items[3]) * Tools.itemSet.get("sugjs") + atof(items[4]) * Tools.itemSet.get("dtxh")
							+ atof(items[5]) * Tools.itemSet.get("dtsc") + atof(items[6]) * Tools.itemSet.get("xzgq")
							+ atof(items[7]) * Tools.itemSet.get("tjdgd"));
				}
				sb.append(rate).append("|").append(Math.round(score) > 100 ? 99 : Math.round(score)).append(",");
			}

			String result = sb.toString();
			if (result.length() > 0) {
				context.write(key, new Text(result.substring(0, result.length() - 1)));
			}
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, String input)
			throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		FileSystem hdfs = FileSystem.get(conf);
		String output = "/user/work/evan/output/GetSongRelatedScore";
		hdfs.delete(new Path(output), true);

		Job job = Job.getInstance(conf);
		job.setJarByClass(GetSongRelatedScore.class);
		job.setJobName("Evan_GetSongRelatedScore");
		job.setNumReduceTasks(10);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReduce.class);
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

		GetSongRelatedScore.runLoadMapReducue(conf, args[0]);
	}
}