package com.baidumusic.yujie;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

import com.alibaba.fastjson.JSONObject;

public class UGCaddGD {

	private final static String TAB = "|";

	// 得到MSG里面的内容
	public static String getMsg(String url) {
		if (url.matches(".*msg\\[.*\\].*")) {
			int start_msg_index = url.indexOf("msg[");
			int end_msg_index = url.indexOf("]", start_msg_index);
			return url.substring(start_msg_index + 4, end_msg_index);
		} else {
			return "";
		}
	}

	// 得到IP地址
	public static String getIp(String url) {
		if (url.matches(".*ip\\[.*\\].*")) {
			int start_ip_index = url.indexOf("ip[");
			int end_ip_index = url.indexOf("]", start_ip_index);
			return url.substring(start_ip_index + 3, end_ip_index);
		} else {
			return "";
		}
	}

	// 得到评论类型 comment_type_id
	public static String getComment_type_id(String url) {
		if (url.matches(".*comment_type_id\\[.*\\].*")) {
			int start_comment_type = url.indexOf("comment_type_id[");
			int end_comment_type = url.indexOf("]", start_comment_type);
			return url.substring(start_comment_type + 16, end_comment_type);
		} else {
			return "";
		}
	}

	// 得到评论的thread_id
	public static String getThread_id(String url) {
		if (url.matches(".*thread_id\\[.*\\].*")) {
			int start_thread = url.indexOf("thread_id[");
			int end_thread = url.indexOf("]", start_thread);
			return url.substring(start_thread + 10, end_thread);
		} else {
			return "";
		}
	}

	// 得到uid
	public static String getUid(String url) {
		if (url.matches(".*uid\\[.*\\].*")) {
			int start_uid = url.lastIndexOf("uid[");
			int end_uid = url.indexOf("]", start_uid);
			return url.substring(start_uid + 4, end_uid);
		} else {
			return "";
		}
	}

	// 得到时间
	public static String getDate(String url) {
		int end_date = url.indexOf("/");
		return url.substring(end_date - 18, end_date - 1);
	}

	// 得到点赞类型
	public static String getFav_id(String url) {
		if (url.matches(".*fav_type\\[.*\\].*")) {
			int start_fav = url.indexOf("fav_type[");
			int end_fav = url.indexOf("]", start_fav);
			return url.substring(start_fav + 9, end_fav);
		} else {
			return "";
		}
	}

	// 添加歌曲到歌单
	public static Map<String, String> getUrlparams(String url) {
		Map<String, String> urlparams = new HashMap<String, String>();
		if (url.matches(".*\\?.*=.+")) {
			String[] args = url.split("\\?");
			String new_url = args[1];
			String[] args1 = new_url.split("&");
			for (int i = 0; i < args1.length; i++) {
				String[] args2 = args1[i].split("=");
				if (args2.length == 2) {
					urlparams.put(args2[0], args2[1]);
				} else {
					urlparams.put(args2[0], "");
				}
			}
			return urlparams;
		} else {
			return null;
		}

	}

	public static class ugcMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			if (value.toString().matches(".*Http_read_suc_/diybase/diysong/addSongToDiy.*")) {
				Map<String, String> urlparams = new HashMap<String, String>();
				String date = getDate(value.toString()).isEmpty() ? "null" : getDate(value.toString());
				String ip = getIp(value.toString()).isEmpty() ? "null" : getIp(value.toString());
				urlparams = getUrlparams(getMsg(value.toString()));
				if (urlparams != null) {
					String list_id = urlparams.get("list_id") == null ? "null" : urlparams.get("list_id");
					String song_ids = urlparams.get("song_ids") == null ? "null" : urlparams.get("song_ids");
					String user_id = urlparams.get("user_id") == null ? "null" : urlparams.get("user_id");
					JSONObject jsonObject = new JSONObject();
					String result = ip + TAB + song_ids + TAB + list_id;
					jsonObject.put(date, result);
					context.write(new Text(user_id), new Text(jsonObject.toString()));
				}
				// context.write(new Text(date),new Text(ip));
			}

		}

	}

	public static class ugcReduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			StringBuilder sb = new StringBuilder();
			int cnt = 0;
			for (Text val : values) {
				if (cnt++ == 0) {
					sb.append(val.toString());
				} else {
					sb.append(";").append(val.toString());
				}
			}
			context.write(key, new Text(sb.toString()));

		}
	}

	public static boolean runLoadMapReducue(Configuration conf, String input, Path output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(UGCaddFav.class);
		job.setJobName("UGC_ADDGD");
		job.setNumReduceTasks(1);
		job.setMapperClass(ugcMapper.class);
		job.setReducerClass(ugcReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);
		return job.waitForCompletion(true);
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		if (args.length == 0) {
			System.err.println("Usage: rcfile <in>");
			System.exit(1);
		}

		String queue = "mapred";
		if (args.length > 1) {
			queue = args[1].matches("hql|dstream|mapred|udw|user|common") ? args[1] : "mapred";
		}
		conf.set("mapreduce.job.queuename", queue);

		FileSystem hdfs = FileSystem.get(conf);
		String out = "/user/work/yujie/tmp/ADDGD";
		Path path = new Path(out);
		hdfs.delete(path, true);
		UGCaddGD.runLoadMapReducue(conf, args[0], new Path(out));
	}
}
