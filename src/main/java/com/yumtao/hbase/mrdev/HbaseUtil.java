package com.yumtao.hbase.mrdev;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
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

import com.yumtao.hbase.mrdev.vo.FamilyColumnVal;

/**
 * HBase java API CRUD util
 * 
 * @author yumTao
 *
 */
public class HbaseUtil {

	private static Configuration config;
	private static Connection connection;
	private static Admin admin;

	public static String FAMILY_COLUMN_VAL_SPLIT = "\001";

	/**
	 * 1、配置加载。
	 * 2、 获取连接。
	 * 3、获取表管理类。
	 */
	static {
		try {
			config = HBaseConfiguration.create();
			config.set("hbase.zookeeper.quorum", "singlenode");
			connection = ConnectionFactory.createConnection(config);
			admin = connection.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void createTable(String tname, String... familys) throws Exception {
		if (familys.length == 0) {
			throw new RuntimeException("family is empty");
		}
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tname));
		for (String family : familys) {
			desc.addFamily(new HColumnDescriptor(family));
		}
		admin.createTable(desc);
	}

	public static void dropTable(String tname) throws Exception {
		TableName tableName = TableName.valueOf(tname);
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(TableName.valueOf(tname));
		}
	}

	public static void insertData(String tname, String rowkey, String... familyColumnVals) throws Exception {
		if (familyColumnVals.length == 0) {
			throw new RuntimeException("familyColumnVals is empty");
		}

		Table table = connection.getTable(TableName.valueOf(tname));
		Put put = new Put(Bytes.toBytes(rowkey));

		Arrays.asList(familyColumnVals).stream().forEach(familyColumnVal -> {
			FamilyColumnVal fcvObj = FamilyColumnVal.getFamilyColumnVal(familyColumnVal);
			String family = fcvObj.getFamily();
			String column = fcvObj.getColumn();
			String value = fcvObj.getValue();
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
		});

		table.put(put);
	}

	public static void deleteData(String tname, String rowkey, String familyColumnVal) throws Exception {
		if (StringUtils.isEmpty(tname) || StringUtils.isEmpty(rowkey)) {
			throw new RuntimeException("tname or rowkey is empty");
		}
		Table table = connection.getTable(TableName.valueOf(tname));
		Delete delete = new Delete(Bytes.toBytes(rowkey));

		FamilyColumnVal fcvObj = FamilyColumnVal.getFamilyColumnVal(familyColumnVal);
		String family = fcvObj.getFamily();
		String column = fcvObj.getColumn();

		if (StringUtils.isNotEmpty(family)) {
			delete.addFamily(Bytes.toBytes(family));
		}
		if (StringUtils.isNotEmpty(column)) {
			delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
		}
		table.delete(delete);
	}

	public static Result getData(String tname, String rowkey, String familyColumnVal) throws Exception {
		Table table = connection.getTable(TableName.valueOf(tname));
		Get get = new Get(Bytes.toBytes(rowkey));

		FamilyColumnVal fcvObj = FamilyColumnVal.getFamilyColumnVal(familyColumnVal);
		String family = fcvObj.getFamily();
		String column = fcvObj.getColumn();

		if (StringUtils.isNotEmpty(family)) {
			get.addFamily(Bytes.toBytes(family));
		}

		if (StringUtils.isNotEmpty(family) && StringUtils.isNotEmpty(column)) {
			get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
		}

		Result result = table.get(get);
		return result;
	}

	public static ResultScanner scanData(String tname) throws Exception {
		Table table = connection.getTable(TableName.valueOf(tname));
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		return scanner;
	}

}
