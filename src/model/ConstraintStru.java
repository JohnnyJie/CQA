package model;

import java.util.ArrayList;
import java.util.HashMap;

public class ConstraintStru {
	private ArrayList<HashMap> vioTupleMap;
	private String depSql;
	private ArrayList<TableStru> tableList;
	private String sequence;

	public ConstraintStru(ArrayList<HashMap> vioTupleMap, String depSql, ArrayList<TableStru> tableList, String sequence){
		this.vioTupleMap = vioTupleMap;
		this.depSql = depSql;
		this.tableList = tableList;
		this.sequence =sequence;
	}

	public ArrayList<HashMap> getVioTupleMap() {
		return vioTupleMap;
	}

	public String getDepSql() {
		return depSql;
	}

	public String getSequence() {
		return sequence;
	}
	public ArrayList<TableStru> getTableList() {
		return tableList;
	}
}
