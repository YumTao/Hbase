package com.yumtao.hbase.mrdev;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @goal 读取hbase student表数据，分词统计后写入到hbase的statistics表中。
 * @init 初始化，创建student，statistics表，插入模拟数据到student中。
 * @mapper 读取 student 表数据，分词按次数写出。
 * @reducer 统计单词出现次数，写入到statistics表中。
 * 
 * @author yumTao
 *
 */
public class WordCountInHbaseMR {

	private static final Logger log = LoggerFactory.getLogger(WordCountInHbaseMR.class);
	private static String srcTable = "student";
	private static String srcTableFamily1 = "bodymsg";
	private static String srcTableFamily2 = "jobmsg";
	private static String destTable = "statistics";
	private static String destTableFamily = "word";

	/**
	 * TableMapper<KEYOUT, VALUEOUT>
	 * source from hbase
	 * @author yumTao
	 *
	 */
	static class WordCountInHbaseMapper extends TableMapper<Text, IntWritable> {
		
		private Map<String, Integer> word2Count = new HashMap<>();

		// 输入的类型为：key：rowKey; value：一行数据的结果集Result (以列为单位，不同列视为不同行， 一个列一个result)
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			List<Cell> cells = value.listCells();

			cells.stream().forEach(cell -> {
				String family = Bytes.toString(CellUtil.cloneFamily(cell));
				String column = Bytes.toString(CellUtil.cloneQualifier(cell));
				String columnVal = Bytes.toString(CellUtil.cloneValue(cell));
				log.debug("family:{}, column:{}, columnVal:{}", family, column, columnVal);
				try {
					String[] wordArray = columnVal.split(" ");
					Arrays.asList(wordArray).stream().forEach(word -> {
						int count = word2Count.get(word) == null ? 0 : word2Count.get(word).intValue();
						word2Count.put(word, count + 1);
					});
					
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		}

		@Override
		protected void cleanup(Mapper<ImmutableBytesWritable, Result, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			for(String word : word2Count.keySet()) {
				context.write(new Text(word), new IntWritable(word2Count.get(word)));
			}
		}
		
		

	}

	/**
	 * TableReducer<KEYIN, VALUEIN, KEYOUT>
	 * sink to hbase
	 * 
	 * @author yumTao
	 *
	 */
	static class WordCountInHbaseReduce extends TableReducer<Text, IntWritable, NullWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable intWritable : values) {
				count += intWritable.get();
			}
			
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.addColumn(Bytes.toBytes("word"), Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(count)));
			context.write(NullWritable.get(), put);
		}

	}

	public static void main(String[] args) throws Exception {

		dataInit();

		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "singlenode");
		conf.set("mapreduce.framework.name", "local");
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resourcemanager.hostname", "singlenode");
		
		// 创建job
		Job job = Job.getInstance(conf);
		job.setJarByClass(WordCountInHbaseMR.class);

		// 创建scan
		Scan scan = new Scan();

		// 创建查询hbase的mapper，
		TableMapReduceUtil.initTableMapperJob(
				srcTable, 			// 设置表名
				scan, 				// scan
				WordCountInHbaseMapper.class, 	// mapper类
				Text.class, 		// mapper的输出key
				IntWritable.class, 	// mapper的输出value
				job);				// MR job
		
		// 创建写入hbase的reducer
		TableMapReduceUtil.initTableReducerJob(
				destTable, 		// 指定表名
				WordCountInHbaseReduce.class, 	// reducer类
				job);				// MR job

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	/**
	 * @INIT 表创建，数据构造
	 * @throws Exception
	 */
	public static void dataInit() throws Exception {
		
		HbaseUtil.dropTable(srcTable);
		HbaseUtil.createTable(srcTable, srcTableFamily1, srcTableFamily2);

		StringBuilder bodymsg = new StringBuilder();
		bodymsg.append(srcTableFamily1).append("\001").append("height").append("\001")
				.append("wowo`s height is 174, because he often play basketball at school");

		StringBuilder bodymsg1 = new StringBuilder();
		bodymsg1.append(srcTableFamily1).append("\001").append("height").append("\001")
		.append("wowo`s height is 175, because he often play basketball at school");
		
		StringBuilder bodymsg2 = new StringBuilder();
		bodymsg2.append(srcTableFamily1).append("\001").append("weight").append("\001")
		.append("wowo`s height is 56, he is thin");
		
		StringBuilder jobmsg = new StringBuilder();
		jobmsg.append(srcTableFamily2).append("\001").append("name").append("\001")
				.append("wowo`s job in bank and scally is much");
		
		HbaseUtil.insertData(srcTable, "wowo", bodymsg.toString(), bodymsg2.toString(), jobmsg.toString(), bodymsg1.toString());
		
		
		HbaseUtil.dropTable(destTable);
		HbaseUtil.createTable(destTable, destTableFamily);
	}

}
