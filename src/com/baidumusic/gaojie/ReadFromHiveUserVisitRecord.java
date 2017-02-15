package com.baidumusic.gaojie;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hive.hcatalog.rcfile.RCFileMapReduceInputFormat;
import org.json.JSONObject;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;

public class ReadFromHiveUserVisitRecord {
	private static final String TAB = "\t";

	private static class RcFileMapper extends Mapper<Object, BytesRefArrayWritable, Text, Text> {

		private static String getValue(BytesRefArrayWritable value, int index) {
			Text txt = new Text();
			BytesRefWritable val = value.get(index);
			try {
				txt.set(val.getData(), val.getStart(), val.getLength());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return txt.toString();
		}
		protected void map(Object key, BytesRefArrayWritable value, Context context)
				throws IOException, InterruptedException {
			String baiduid = getValue(value, 8);
			String userid = getValue(value, 9);
			String str_domain = getValue(value, 6);
			if (str_domain.equals("http://y.baidu.com") || str_domain.equals("y.baidu.com"))
				return;

			String url = getValue(value, 14);
			String str_url = "";
			try {
				str_url = java.net.URLDecoder.decode(url, "UTF-8").replace(TAB, "");
			} catch (Exception e) {
				str_url = Escape.unescape(url);
			}

			//if (str_url.contains("/scene/cmd=rpp") || str_url.contains("..%2F..%2F")) return;
			if (!(str_domain.equals("fm.baidu.com") 
					|| str_url.matches("^\\/?(song|artist|songlist|album)\\/[0-9]+.*$")
					|| str_url.matches("^\\/?search.*$")
					|| str_url.contains("pst=sug")))
				return;
			
			if (baiduid.matches("^[0-9a-zA-Z]+$") || userid.matches("^[0-9]{2,}$")) {
				String str_time = getValue(value, 0);
				String str_ip = getValue(value, 2);
				String urlpath = getValue(value, 15);
				String str_urlpath = "";
				try {
					str_urlpath = java.net.URLDecoder.decode(urlpath, "UTF-8").replace(TAB, "");
				} catch (Exception e) {
					str_urlpath = Escape.unescape(urlpath);
				}

				/* String refer_urlpath = getValue(value, 12);
				String str_refer_urlpath = "";
				try {
					str_refer_urlpath = java.net.URLDecoder.decode(refer_urlpath, "UTF-8").replace(TAB, "");
				} catch (Exception e) {
					str_refer_urlpath = Escape.unescape(refer_urlpath);
				}

				BytesRefWritable refer_domain = value.get(13);
				txt.set(refer_domain.getData(), refer_domain.getStart(), refer_domain.getLength());
				String str_refer_domain = getValue(value, 13).equals("空") ? "null" : getValue(value, 13); */

				String uniq_id = userid.matches("^[0-9]{2,}$") ? userid : baiduid;

				if (str_domain.equals("fm.baidu.com")) {
					// 分别处理访问日志中的电台
					context.write(new Text(uniq_id),
							new Text("visit" + TAB + str_time + TAB + str_ip + TAB + "fm" + TAB + "0"));
				} else if (str_urlpath.equals("/search")) {
					// 分别处理访问日志中的普通检索
					context.write(new Text(uniq_id), new Text("search" + TAB + str_time + TAB + str_ip));
				} else if (str_url.contains("pst=sug")) {
					// 分别处理访问日志中的SUG检索
					String[] arr_path = str_urlpath.split("\\/");
					if (3 == arr_path.length) {
						context.write(new Text(uniq_id), new Text(
								"sug" + TAB + str_time + TAB + str_ip + TAB + arr_path[1] + TAB + arr_path[2]));
					}
				} else if (str_url.matches("^\\/?(song|artist|songlist|album)\\/[0-9]+.*$")) {
					// 分别处理访问日志中的歌曲、歌手 、歌单、专辑
					String[] arr_path = str_urlpath.split("\\/");
					if (3 == arr_path.length) {
						context.write(new Text(uniq_id), new Text(
								"visit" + TAB + str_time + TAB + str_ip + TAB + arr_path[1] + TAB + arr_path[2]));
					}
				}
			}
		}
	}

	private static class RcFileReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			JSONObject json_sug = new JSONObject();
			JSONObject json_visit = new JSONObject();
			JSONObject json_search = new JSONObject();
			try {
				for (Text txt : values) {
					String str_temp = txt.toString();
					String[] arr_temp = str_temp.split(TAB);
					if (arr_temp.length > 1) {
						if (arr_temp[0].equals("sug")) {
							json_sug.put(arr_temp[1], str_temp.substring(4));
						} else if (arr_temp[0].equals("visit")) {
							json_visit.put(arr_temp[1], str_temp.substring(6));
						} else if (arr_temp[0].equals("search")) {
							json_search.put(arr_temp[1], str_temp.substring(7));
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			String str_value = "";
			if (0 == json_visit.length()) {
				str_value = "no-visit";
			} else {
				str_value = json_visit.toString();
			}
			if (0 == json_search.length()) {
				str_value += TAB + "no-search";
			} else {
				str_value += TAB + json_search.toString();
			}
			if (0 == json_sug.length()) {
				str_value += TAB + "no-sug";
			} else {
				str_value += TAB + json_sug.toString();
			}

			context.write(key, new Text(str_value));
		}
	}
	
	public static boolean runLoadMapReducue(Configuration conf, Path input, Path output)
			throws IOException, ClassNotFoundException, InterruptedException {
		conf.set("hive.io.file.read.all.columns", "false");
		// time ip domain baiduid userid refer refer_urlpath refer_domain url
		// urlpath
		conf.set("hive.io.file.readcolumn.ids", "0,2,6,8,9,11,12,13,14,15");
		Job job = Job.getInstance(conf);
		job.setJarByClass(ReadFromHiveUserVisitRecord.class);
		job.setJobName("ReadFromHiveUserVisitRecord");
		job.setNumReduceTasks(10);
		job.setMapperClass(RcFileMapper.class);
		job.setReducerClass(RcFileReduce.class);
		job.setInputFormatClass(RCFileMapReduceInputFormat.class);
		RCFileMapReduceInputFormat.addInputPath(job, input);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		FileOutputFormat.setOutputPath(job, output);
		return job.waitForCompletion(true);
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String queue = "mapred";
		String reducers = "1";
		if (args.length > 2) {
			reducers = args[2].matches("[0-9]+") ? args[2] : "1";
		}
		if (args.length > 1) {
			queue = args[1].matches("hql|dstream|mapred|udw|user|common") ? args[1] : "mapred";
		}
		conf.set("mapreduce.job.queuename", queue);
		conf.set("mapreduce.reducers", reducers);

		if (args.length == 0) {
			System.err.println("Usage: class <in>");
			System.exit(1);
		}
		FileSystem hdfs = FileSystem.get(conf);
		String out = "/user/work/yan/ReadFromHiveUserVisitRecord";
		Path path = new Path(out);
		hdfs.delete(path, true);

		ReadFromHiveUserVisitRecord.runLoadMapReducue(conf, new Path(args[0]), new Path(out));
	}
}