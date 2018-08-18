package model;

import java.util.ArrayList;

/**
 * 
 * @author Xueqi
 *
 */
public class QueriesStru {
	private String select;
	private String from;
	private String where;
	private ArrayList<String> att;
	private ArrayList<String> tablelist;
	private String[] constrains;

	public String getSelect() {
		return select;
	}

	public void setSelect(String select) {
		this.select = select;
	}

	public String getFrom() {
		return from;
	}

	public void setFrom(String from) {
		this.from = from;
	}

	public String getWhere() {
		return where;
	}

	public void setWhere(String where) {
		this.where = where;
	}

	public ArrayList<String> getAtt() {
		return att;
	}

	public void setAtt(ArrayList<String> attlist) {
		this.att = attlist;
	}

	public ArrayList<String> getTablelist() {
		return tablelist;
	}

	public void setTablelist(ArrayList<String> tablelist) {
		this.tablelist = tablelist;
	}

	public String[] getConstrains() {
		return constrains;
	}

	public void setConstrains(String[] constrains) {
		this.constrains = constrains;
	}

}
