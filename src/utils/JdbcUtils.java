package utils;

import dao.BaseDao;
import model.QueriesStru;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * 
 * @author Xueqi
 *
 */
public class JdbcUtils {

	public QueriesStru stru = new QueriesStru();
	public BaseDao baseDao = new BaseDao();

	public String queryPath = "/Users/qq/Documents/GitHub/DPMT/sql/query.sql"; // path of query

	/**
	 * Get context of file
	 * 
	 * @param path
	 * @return
	 */
	public String getText(String path) {
		File file = new File(path);
		if (!file.exists() || file.isDirectory()) {
			return null;
		}
		StringBuilder sb = new StringBuilder();
		try {
			FileInputStream fis = new FileInputStream(path);
			InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
			BufferedReader br = new BufferedReader(isr);
			String temp = null;
			temp = br.readLine();
			while (temp != null) {
				if (temp.length() >= 2) {
					String str1 = temp.substring(0, 1);
					String str2 = temp.substring(0, 2);
					if (str1.equals("#") || str2.equals("--") || str2.equals("/*") || str2.equals("//")) {
						temp = br.readLine();
						continue;
					}
					sb.append(temp + "\r\n");
				}

				temp = br.readLine();
			}
			br.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return sb.toString();
	}

	/**
	 * 
	 * @param sqlText
	 * @return QueriesStru stru
	 */
	public QueriesStru splitQuery(String sqlText, Connection conn) {

		String tmp;
		String[] tmp2;
		String[] splitsql;
		String[] split2;
		ArrayList<String> attlist = new ArrayList<>(); // store attribute required in the query
		ArrayList<String> tablelist = new ArrayList<>();
		sqlText = sqlText.toLowerCase(); // get context of the query and turn all the text

		//// split the query and get all the attribute we want to select
		splitsql = sqlText.split("from");
		stru.setSelect(splitsql[0].trim());
		split2 = splitsql[0].trim().split("\\s+", 2);
		tmp = split2[1];
		split2 = tmp.split(",");
		for (int i = 0; i < split2.length; i++) {
			if (!split2[i].isEmpty() && !split2[i].equals(" ") && !split2[i].equals("distinct")) {
				attlist.add(split2[i].trim());

			}
		}
		stru.setAtt(attlist);

		// Get all the table name
		splitsql = splitsql[1].split("where");
		// System.out.println(splitsql[0]);
		stru.setFrom(splitsql[0].trim());
		split2 = splitsql[0].trim().split(",");

		for (int i = 0; i < split2.length; i++) {
			// judge is it a conjunctive query
			if (split2[i].contains("join") && split2[i].contains("on")) {
				tmp2 = split2[i].trim().split("join");
				for (String tablesql : tmp2) {
					String[] tmp3 = tablesql.trim().split(" |\n");
					tablelist.add(tmp3[0].trim());
				}
			} else {
				tablelist.add(split2[i].trim());
			}
		}
		stru.setTablelist(tablelist);
		if (stru.getAtt().get(0).equals("*")) {
			ArrayList<String> AttName = new ArrayList<>();
			for (String tablename : tablelist) {
				AttName.addAll(baseDao.getColumnNames(tablename, conn));
			}
			stru.setAtt(AttName);
		}
		// set constraints
		// if query does not contain constraints, set where as "null"
		if (splitsql.length != 1) {
			tmp = splitsql[1].trim().substring(splitsql[1].trim().length() - 1, splitsql[1].trim().length());
			// System.out.println("tmp:" + tmp);
			if (tmp.equals(";")) {
				stru.setWhere(splitsql[1].trim().substring(0, splitsql[1].trim().length() - 1));
			} else {
				stru.setWhere(splitsql[1].trim());
			}

		} else {
			stru.setWhere("null");
		}

		return stru;
	}

	/**
	 * Create a new View of table which not include the deleted tuples
	 * 
	 * @param conn
	 * @param tableNames:list
	 *            the original table names
	 */
	public void CreateDeleteView_NOTIN(Connection conn, ArrayList<String> tableNames) {

		// CREATE VIEW NEW_tableName AS
		// SELECT *
		// FROM tableName
		// WHERE (attributes,...) NOT IN (SELECT * FROM del_tableName)
		for (String tableName : tableNames) {
			String sql = "CREATE VIEW NEW_" + tableName + " AS \nSELECT * \nFROM " + tableName + "\nWHERE (";
			ArrayList<String> columnNames = baseDao.getColumnNames(tableName, conn);
			for (String columName : columnNames) {
				sql += columName + ", ";
			}
			sql = sql.substring(0, sql.length() - 2);
			sql += ") NOT IN (SELECT * FROM del_" + tableName + ");";
			baseDao.executeSQL(sql, conn);
		}

	}

	/**
	 * Create a new View of table which not include the deleted tuples
	 * 
	 * @param conn
	 * @param tableNames:list
	 *            the original table names
	 */
	public void CreateDeleteView_NOTEXIST(Connection conn, ArrayList<String> tableNames) {
		// CREATE VIEW NEW_tableName AS
		// SELECT *
		// FROM tableName
		// WHERE NOT EXISTS (SELECT 1 FROM del_tableName)
		for (String tableName : tableNames) {
			String sql = "CREATE VIEW NEW_" + tableName + " AS \nSELECT * \nFROM " + tableName
					+ "\nWHERE NOT EXISTS (SELECT 1\nFROM del_" + tableName + "\nWHERE ";
			ArrayList<String> AttNames = baseDao.getColumnNames(tableName, conn);
			for (String AttName : AttNames) {
				sql += tableName + "." + AttName + " = del_" + tableName + "." + AttName + " AND ";
			}
			sql = sql.substring(0, sql.length() - 5);
			sql += ");";

			baseDao.executeSQL(sql, conn);
		}

	}

	/**
	 * Drop new views created when running the program
	 * 
	 * @param conn
	 * @param tableNames
	 */
	public void DropDView(Connection conn, ArrayList<String> tableNames) {
		String sql = null;
		for (String tableName : tableNames) {
			// DROP VIEW NEW_tableName
			sql = "DROP VIEW NEW_" + tableName + ";";
			baseDao.executeSQL(sql, conn);

		}

	}

	/**
	 * Drop new tables created when running the program
	 * 
	 * @param conn
	 * @param tableNames
	 */
	public void DropDTable(Connection conn, ArrayList<String> tableNames) {

		for (String tableName : tableNames) {
			if (baseDao.validateTableNameExist("del_" + tableName, conn)) {
				// DROP TABLE del_tableName
				String sql = "DROP TABLE del_" + tableName + ";";
				 System.out.println(sql);
				baseDao.executeSQL(sql, conn);
			}

		}

	}

	/**
	 * Create new delete table for each table
	 * 
	 * @param conn
	 */
	public void createDeleteTbale(Connection conn, ArrayList<String> tableNames) {

		for (String tableName : tableNames) {
			// System.out.println(tableName);
			String judge = tableName.substring(0, 3);
			if (!judge.equals("del")) {
				/*
				 * if (baseDao.validateTableNameExist("del_" + tableName, conn)) { String sql =
				 * "DROP TABLE del_" + tableName + ";"; baseDao.executeSQL(sql, conn); }
				 */
				String sql = "CREATE TABLE del_" + tableName + "(";
				ArrayList<String> columnNames = baseDao.getColumnNames(tableName, conn);
				ArrayList<String> columnTypes = baseDao.getColumnTypes(tableName, conn);
				int i = 0;
				for (String columnName : columnNames) {
					String columnType = columnTypes.get(i++);
					sql += columnName + " " + columnType + ",";
				}
				sql = sql.substring(0, sql.length() - 1);
				sql += ");";
				// System.out.println(sql);
				baseDao.executeSQL(sql, conn);

			}

		}

	}

	/*****
	 *
	 * @param Dlist
	 * @param conn
	 */
	public void InsertData(ArrayList<HashMap> Dlist, Connection conn) {
		HashMap<String, String> Data = new HashMap<>();
		for (HashMap tuple : Dlist) {

			String data = "";
			ArrayList<String> columnNames = baseDao.getColumnNames((String) tuple.get("tableName"), conn);
			for (String columnName : columnNames) {

				data += " '" + tuple.get(columnName) + "', ";
			}
			data = data.substring(0, data.length() - 2);
			Data.put(data, (String) tuple.get("tableName"));
			//baseDao.insertdata(conn, (String) tuple.get("tableName"), data);
		}

		// System.out.println("data:" + data);
		baseDao.insertBanchdata(conn, Data);
	}

	public HashMap<ArrayList<String>, Integer> queryRewrite(QueriesStru stru, Connection conn,
			HashMap<ArrayList<String>, Integer> tupleList) {

		Statement stmt = null;
		ResultSet rs = null;

		String sql = stru.getSelect() + "\nFROM " + stru.getFrom();
		ArrayList<String> tablelist = stru.getTablelist();
		for (String tableName : tablelist) {
			sql.replace(tableName, "NEW_"+tableName);
		}

		if (stru.getWhere() != "null") {
			sql += "\nWHERE " + stru.getWhere();
		}
		// System.out.println(sql);
		try {
			stmt = conn.createStatement();
			System.out.println(sql);
			rs = stmt.executeQuery(sql);
			while (rs.next()) {

				ArrayList<String> tuple = new ArrayList<>();
				// System.out.println(stru.getAtt().size());
				for (int i = 1; i <= stru.getAtt().size(); i++) {
					tuple.add(rs.getString(i));
				}
				if (tupleList.containsKey(tuple)) {
					int count = tupleList.get(tuple);
					count++;
					tupleList.put(tuple, count);
				} else {
					tupleList.put(tuple, 1);
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return tupleList;
	}

}
