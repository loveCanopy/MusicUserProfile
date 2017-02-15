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

public class ParseUGCLog {

	private final static String TAB = "\t";

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

	// 得到点赞内容ID
	public static String getFav_content_id(String url) {
		if (url.matches(".*fav_content_id\\[.*\\].*")) {
			int start_fav_content = url.indexOf("fav_content_id[");
			int end_fav_content = url.indexOf("]", start_fav_content);
			return url.substring(start_fav_content + 15, end_fav_content);
		} else {
			return "";
		}

	}

	public static class ugcMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			/*
			 * 添加到歌单
			 */
			if (value.toString().matches(".*Http_read_suc_/diybase/diysong/addSongToDiy.*")) {
				Map<String, String> urlparams = new HashMap<String, String>();
				String date = getDate(value.toString()).isEmpty() ? "null" : getDate(value.toString());
				String ip = getIp(value.toString()).isEmpty() ? "null" : getIp(value.toString());
				urlparams = getUrlparams(getMsg(value.toString()));
				if (urlparams != null) {
					String list_id = urlparams.get("list_id") == null ? "null" : urlparams.get("list_id");
					String song_ids = urlparams.get("song_ids") == null ? "null" : urlparams.get("song_ids");
					String user_id = urlparams.get("user_id") == null ? "null" : urlparams.get("user_id");
					context.write(new Text(user_id),
							new Text("gedan" + TAB + ip + TAB + date + TAB + list_id + TAB + song_ids));
				}

			}
			/*
			 * 评论
			 */
			else if (value.toString().matches(".*ugc_addcomment.*")) {
				// String type="发表评论";
				// String comment_type=null;
				// 日期 ip uid 评论类型、thread_id
				String date = getDate(value.toString()).isEmpty() ? "null" : getDate(value.toString());
				String ip = getIp(value.toString()).isEmpty() ? "null" : getIp(value.toString());
				String uid = getUid(value.toString()).isEmpty() ? "null" : getUid(value.toString());
				String comment_type_id = getComment_type_id(value.toString()).isEmpty() ? "null"
						: getComment_type_id(value.toString());
				String thread_id = getThread_id(value.toString()).isEmpty() ? "null" : getThread_id(value.toString());
				context.write(new Text(uid),
						new Text("pinglun" + TAB + ip + TAB + date + TAB + comment_type_id + TAB + thread_id));
			}
			/*
			 * 点赞
			 * 
			 */
			else if (value.toString().matches(".*ugc_addFav.*")) {
				String date = getDate(value.toString()).isEmpty() ? "null" : getDate(value.toString());
				String ip = getIp(value.toString()).isEmpty() ? "null" : getIp(value.toString());
				String uid = getUid(value.toString()).isEmpty() ? "null" : getUid(value.toString());
				String fav_id = getFav_id(value.toString()).isEmpty() ? "null" : getFav_id(value.toString());
				String fav_content_id = getFav_content_id(value.toString()).isEmpty() ? "null"
						: getFav_content_id(value.toString());
				context.write(new Text(uid),
						new Text("dianzan" + TAB + ip + TAB + date + TAB + fav_id + TAB + fav_content_id));
			}

		}
	}

	public static class ugcReduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text arg0, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context arg2)
				throws IOException, InterruptedException {

			JSONObject jsonGd = new JSONObject();
			JSONObject jsoncomment = new JSONObject();
			JSONObject jsonfav = new JSONObject();
			for (Text val : values) {
				String value = val.toString();
				String[] valuearr = value.split(TAB);
				if (valuearr.length > 1) {
					if (valuearr[0].equals("dianzan")) {
						String date = valuearr[2];
						jsonfav.put(date, value.substring(8));
					} else if (valuearr[0].equals("pinglun")) {
						String date = valuearr[2];
						jsoncomment.put(date, value.substring(8));
					} else if (valuearr[0].equals("gedan")) {
						String date = valuearr[2];
						jsonGd.put(date, value.substring(6));
					}
				}
			}

			String out_put_fav = "";
			if (jsonfav.isEmpty()) {
				out_put_fav = "no-favor";
			} else {
				out_put_fav = jsonfav.toString();
			}
			String out_put_comment = "";
			if (jsoncomment.isEmpty()) {
				out_put_comment = "no-comment";
			} else {
				out_put_comment = jsoncomment.toString();
			}
			String out_put_gd = "";
			if (jsonGd.isEmpty()) {
				out_put_gd = "no-gedan";
			} else {
				out_put_gd = jsonGd.toString();
			}
			arg2.write(arg0, new Text(out_put_fav + TAB + out_put_comment + TAB + out_put_gd));
		}

	}

	public static boolean runLoadMapReducue(Configuration conf, String input, Path output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);
		job.setJarByClass(ParseUGCLog.class);
		job.setJobName("ParseUGCLog");
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
		String out = "/user/work/yujie/tmp/ParseUGCLog";
		Path path = new Path(out);
		hdfs.delete(path, true);
		ParseUGCLog.runLoadMapReducue(conf, args[0], new Path(out));

	}

}
