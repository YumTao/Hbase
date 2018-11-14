package com.yumtao.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * HBase java API CRUD
 * @author yumTao
 *
 */
public class HbaseClient {

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

	@Test
	public void createTable() throws Exception {
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf("jva_dev"));
		HColumnDescriptor libDev = new HColumnDescriptor("lib");
		HColumnDescriptor mvnDev = new HColumnDescriptor("maven");
		desc.addFamily(libDev);
		desc.addFamily(mvnDev);
		admin.createTable(desc);
	}

	@Test
	public void insertData() throws Exception {
		Table table = connection.getTable(TableName.valueOf("jva_dev"));
		Put put = new Put(Bytes.toBytes("eclipse"));
		put.addColumn(Bytes.toBytes("lib"), Bytes.toBytes("jar_mr"), Bytes.toBytes("map and reduce"));
		put.addColumn(Bytes.toBytes("lib"), Bytes.toBytes("jar_hive"), Bytes.toBytes("hive"));
		put.addColumn(Bytes.toBytes("maven"), Bytes.toBytes("mvn_mr"), Bytes.toBytes("mvn_map and reduce"));
		put.addColumn(Bytes.toBytes("maven"), Bytes.toBytes("mvn_hive"), Bytes.toBytes("hive"));
		table.put(put);
	}

	@Test
	public void insertDataBatch() throws Exception {
		Table table = connection.getTable(TableName.valueOf("jva_dev"));

		// row key --eclipse
		List<Put> putGroup = new ArrayList<>();
		Put put = new Put(Bytes.toBytes("eclipse"));
		put.addColumn(Bytes.toBytes("lib"), Bytes.toBytes("hdfsjar"), Bytes.toBytes("common2"));
		put.addColumn(Bytes.toBytes("maven"), Bytes.toBytes("hdfsmvn"), Bytes.toBytes("mvn_common2"));
		putGroup.add(put);

		// row key --ideaJ
		Put ideaPut = new Put(Bytes.toBytes("ideaJ"));
		ideaPut.addColumn(Bytes.toBytes("lib"), Bytes.toBytes("hdfsjar"), Bytes.toBytes("common1"));
		ideaPut.addColumn(Bytes.toBytes("maven"), Bytes.toBytes("hivemvn"), Bytes.toBytes("mvn_exec1"));
		putGroup.add(ideaPut);
		
		Put test = new Put(Bytes.toBytes("test"));
		test.addColumn(Bytes.toBytes("lib"), Bytes.toBytes("test"), Bytes.toBytes("test"));
		putGroup.add(test);
		table.put(putGroup);
	}

	@Test
	public void deleteData() throws Exception {
		Table table = connection.getTable(TableName.valueOf("jva_dev"));
//		default delete by row key
		Delete delete = new Delete(Bytes.toBytes("ideaj"));
//		delete by column
//		delete.addColumn(Bytes.toBytes("lib"), Bytes.toBytes("hdfsjar"));

//		delete by columnFamily
//		delete.addFamily(Bytes.toBytes("lib"));
		table.delete(delete);
	}

	@Test
	public void getData() throws Exception {
		Table table = connection.getTable(TableName.valueOf("jva_dev"));
//		default use row key query
		Get get = new Get(Bytes.toBytes("eclipse"));
//		query by column
//		get.addColumn(Bytes.toBytes("lib"), Bytes.toBytes("hdfsjar"));
		Result result = table.get(get);
		byte[] value1 = result.getValue(Bytes.toBytes("lib"), Bytes.toBytes("hdfsjar"));
		System.out.println("values: " + Bytes.toString(value1));
		List<Cell> cells = result.listCells();
		cells.stream().forEach(cell -> {
			byte[] cloneFamily = CellUtil.cloneFamily(cell);
			byte[] cloneValue = CellUtil.cloneValue(cell);
			System.out.println(Bytes.toString(cloneFamily));
			System.out.println(Bytes.toString(cloneValue));
		});

		Cell[] rawCells = result.rawCells();
		for (Cell cellTmp : rawCells) {
			byte[] cloneRow = CellUtil.cloneRow(cellTmp);
			byte[] cloneFamily = CellUtil.cloneFamily(cellTmp);
			byte[] cloneColumn = CellUtil.cloneQualifier(cellTmp);
			byte[] cloneValue = CellUtil.cloneValue(cellTmp);
			System.out.println(String.format("row:%s, family:%s, column:%s, value:%s", Bytes.toString(cloneRow),
					Bytes.toString(cloneFamily), Bytes.toString(cloneColumn), Bytes.toString(cloneValue)));
		}
	}

	/**
	 * 批量查询
	 * 
	 * @throws Exception
	 */
	@Test
	public void scanData() throws Exception {
		Table table = connection.getTable(TableName.valueOf("jva_dev"));
		Scan scan = new Scan();
//		scan.setStartRow(Bytes.toBytes("ideaJ"));
		ResultScanner scanner = table.getScanner(scan);
		scanner.forEach(result -> {
			Cell[] rawCells = result.rawCells();
			for (Cell cellTmp : rawCells) {
				byte[] cloneRow = CellUtil.cloneRow(cellTmp);
				byte[] cloneFamily = CellUtil.cloneFamily(cellTmp);
				byte[] cloneColumn = CellUtil.cloneQualifier(cellTmp);
				byte[] cloneValue = CellUtil.cloneValue(cellTmp);
				System.out.println(String.format("row:%s, family:%s, column:%s, value:%s", Bytes.toString(cloneRow),
						Bytes.toString(cloneFamily), Bytes.toString(cloneColumn), Bytes.toString(cloneValue)));
			}
		});
	}

	@After
	public void closeResource() {
		try {
			connection.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
