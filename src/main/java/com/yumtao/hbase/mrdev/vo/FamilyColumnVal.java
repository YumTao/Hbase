package com.yumtao.hbase.mrdev.vo;

import org.apache.commons.lang.StringUtils;

import com.yumtao.hbase.mrdev.HbaseUtil;

public class FamilyColumnVal {

	private String family;
	private String column;
	private String value;

	public static FamilyColumnVal getFamilyColumnVal(String src) {
		FamilyColumnVal result = new FamilyColumnVal();
		if (StringUtils.isNotEmpty(src)) {
			String[] complexs = src.split(HbaseUtil.FAMILY_COLUMN_VAL_SPLIT);
			try {
				result.setFamily(complexs[0]);
				result.setColumn(complexs[1]);
				result.setValue(complexs[2]);
			} catch (Exception e) {
			}
		}

		return result;
	}

	public String getFamily() {
		return family;
	}

	public void setFamily(String family) {
		this.family = family;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

}