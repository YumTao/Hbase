package com.yumtao.hbase.client;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sun.tools.internal.xjc.generator.bean.ImplStructureStrategy.Result;

public class HbaseScanFilter {
	private Configuration config;
	private Connection connection;
	private Admin admin;

	@Before
	public void configInit() throws Exception {
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "singlenode");
//		config.set("hbase.zookeeper.property.clientPort", "2181");
		connection = ConnectionFactory.createConnection(config);
		admin = connection.getAdmin();
	}

	@After
	public void closeResource() {
		try {
			connection.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void test01() throws Exception {
		Table table = connection.getTable(TableName.valueOf("jva_dev"));

//		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);

		filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("lib"), Bytes.toBytes("hdfsjar"),
				CompareOp.EQUAL, Bytes.toBytes("common1")));

		Scan scan = new Scan();
		scan.setRowPrefixFilter(Bytes.toBytes("ecli"));
		scan.setFilter(filterList);

		ResultScanner scanner = table.getScanner(scan);
		scanner.forEach(result -> {
			List<Cell> listCells = result.listCells();
			listCells.stream().forEach(cell -> {
				System.out.println(String.format("row:%s, family:%s, column:%s, value:%s",
						Bytes.toString(CellUtil.cloneRow(cell)), Bytes.toString(CellUtil.cloneFamily(cell)),
						Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell))));
			});
		});

	}

}
