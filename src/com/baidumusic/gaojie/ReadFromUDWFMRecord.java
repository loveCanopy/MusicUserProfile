package com.baidumusic.gaojie;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hive.hcatalog.rcfile.RCFileMapReduceInputFormat;

import com.alibaba.fastjson.JSONObject;
import com.baidumusic.evan.Tools;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

public class ReadFromUDWFMRecord {
	private static final String TAB = "\t";

	private static class RcFileMapper extends Mapper<Object, BytesRefArrayWritable, Text, Text> {

		protected void map(Object key, BytesRefArrayWritable value, Context context)
				throws IOException, InterruptedException {
			Text txt = new Text();
			BytesRefWritable bid = value.get(1);
			txt.set(bid.getData(), bid.getStart(), bid.getLength());
			String baiduid = txt.toString();

			BytesRefWritable uid = value.get(2);
			txt.set(uid.getData(), uid.getStart(), uid.getLength());
			String userid = String.valueOf(BdussSimpleDecoder64.decode64(txt.toString()));

			if (baiduid.matches("^[0-9a-zA-Z]+$") || userid.matches("^[0-9]{2,}$")) {

				BytesRefWritable ip = value.get(10);
				txt.set(ip.getData(), ip.getStart(), ip.getLength());
				String str_ip = txt.toString();

				BytesRefWritable time = value.get(20);
				txt.set(time.getData(), time.getStart(), time.getLength());
				String str_time = txt.toString();

				BytesRefWritable request = value.get(21);
				txt.set(request.getData(), request.getStart(), request.getLength());
				JSONObject json = Tools.splitMap(txt.toString());
				if (json.isEmpty())
					return;

				String ref = json.containsKey("ref") ? (String) json.get("ref") : "";
				String type = json.containsKey("type") ? (String) json.get("type") : "";
				if (ref.equals("radio") && (type.equals("click_singleFavor") || type.equals("click_singleDel"))) {
					String uniq_id = userid.matches("^[0-9]{2,}$") ? userid : baiduid;
					String song_id = json.containsKey("song_id") ? (String) json.get("song_id") : "0";
					context.write(new Text(uniq_id), new Text(type + TAB + str_time + TAB + str_ip + TAB + song_id));
				}
			}
		}
	}

	private static class RcFileReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			JSONObject json_del = new JSONObject();
			JSONObject json_favor = new JSONObject();
			try {
				for (Text txt : values) {
					String str_temp = txt.toString();
					String[] arr_temp = str_temp.split(TAB);
					if (arr_temp.length != 4) 
						continue;
					String type = arr_temp[0];
					if (type.equals("click_singleDel")) {
						json_del.put(arr_temp[1], str_temp.substring(16));
					} else if (type.equals("click_singleFavor")) {
						json_favor.put(arr_temp[1], str_temp.substring(18));
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			String favor_output = "";
			if (json_favor.isEmpty()) {
				favor_output = "no-favor";
			} else {
				favor_output = json_favor.toString();
			}
			String delete_output = "";
			if (json_del.isEmpty()) {
				delete_output = "no-del";
			} else {
				delete_output = json_del.toString();
			}
			context.write(key, new Text(favor_output + TAB + delete_output));
		}
	}

	public static boolean runLoadMapReducue(Configuration conf, Path input, Path output)
			throws IOException, ClassNotFoundException, InterruptedException {
		conf.set("hive.io.file.read.all.columns", "false");
		conf.set("hive.io.file.readcolumn.ids", "1,2,10,20,21"); // baiduid
																	// bduss ip
																	// time url
		Job job = Job.getInstance(conf);
		job.setJarByClass(ReadFromUDWFMRecord.class);
		job.setJobName("ReadFromUDWFMRecord");
		job.setNumReduceTasks(Integer.parseInt(conf.get("mapreduce.reducers")));
		job.setMapperClass(RcFileMapper.class);
		job.setReducerClass(RcFileReduce.class);
		job.setInputFormatClass(RCFileMapReduceInputFormat.class);
		RCFileMapReduceInputFormat.addInputPath(job, input);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
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
		String out = "/user/work/yan/ReadFromUDWFMRecord";
		Path path = new Path(out);
		hdfs.delete(path, true);

		ReadFromUDWFMRecord.runLoadMapReducue(conf, new Path(args[0]), new Path(out));
	}
}