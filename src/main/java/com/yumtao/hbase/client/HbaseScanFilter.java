package com.yumtao.hbase.client;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HbaseScanFilter {
	private Configuration config;
	private Connection connection;
	private ResultScanner scanner;

	@Before
	public void configInit() throws Exception {
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "singlenode");
//		config.set("hbase.zookeeper.property.clientPort", "2181");
		connection = ConnectionFactory.createConnection(config);
	}

	@After
	public void showResultAndClose() throws IOException {
		scanner.forEach(result -> {
			List<Cell> listCells = result.listCells();
			listCells.stream().forEach(cell -> {
				System.out.println(String.format("row:%s, family:%s, column:%s, value:%s",
						Bytes.toString(CellUtil.cloneRow(cell)), Bytes.toString(CellUtil.cloneFamily(cell)),
						Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell))));
			});
		});

		try {
			connection.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void filter() throws Exception {
		Table table = connection.getTable(TableName.valueOf("jva_dev"));
//		多个过滤器&（与关系）
//		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

//		多个过滤器|（或关系）		
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);

		// ColumnPrefixFilter: 单列名前缀过滤器
		filterList.addFilter(new ColumnPrefixFilter(Bytes.toBytes("hdfs")));
		
		// MultipleColumnPrefixFilter: 多个列，列名前缀过滤器
		byte[][] prefixes = new byte[][] {Bytes.toBytes("hdfs"),Bytes.toBytes("test")};
		filterList.addFilter(new MultipleColumnPrefixFilter(prefixes));
		
		// RowFilter: row key 过滤器
		RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("ecli"));
		filterList.addFilter(rowFilter);

		Scan scan = new Scan();
		scan.setFilter(filterList);

		scanner = table.getScanner(scan);

	}

	/**
	 * @NOTICE 过滤出列值满足条件的 row key 所有记录（包括row key 不包含改列名的所有记录）。 如需只显示满足列值的那一列{rowkey, family:column,
	 *         value},请在scan中选择。
	 */
	@Test
	public void singleColumnValFilter() throws Exception {
		Table table = connection.getTable(TableName.valueOf("jva_dev"));
		Filter filter = new SingleColumnValueFilter(Bytes.toBytes("lib"), Bytes.toBytes("hdfsjar"), CompareOp.EQUAL,
				Bytes.toBytes("common2"));

		Scan scan = new Scan();
//		scan.addColumn(Bytes.toBytes("lib"), Bytes.toBytes("hdfsjar"));
		scan.setFilter(filter);

		scanner = table.getScanner(scan);
	}

}
