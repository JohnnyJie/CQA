package dao;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class BaseDao {

	private static final String SQL = "SELECT * FROM ";

	/**
	 * connect to database
	 * 
	 * @return
	 */
	public Connection connectDB(String address, String port, String dbName, String usrName, String psw) {
		Connection c = null;
		try {
			Class.forName("org.postgresql.Driver");
			c = DriverManager.getConnection("jdbc:postgresql://" + address + ":" + port + "/" + dbName, usrName, psw);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
		System.out.println("Opened database successfully");
		return c;
	}

	/**
	 * Close the database connection
	 * 
	 * @param conn
	 */
	public void closeConnection(Connection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println(e.getClass().getName() + ": " + e.getMessage());
				System.exit(0);
			}
		}
	}

	/**
	 * get names of databases
	 */
	public ArrayList<String> getDbName(Connection c) {
		ArrayList<String> dbLst = new ArrayList<>();
		ResultSet rs = null;
		Statement stmt;
		String sql = "SELECT datname FROM pg_database WHERE datistemplate = false";
		try {
			stmt = c.createStatement();
			rs = stmt.executeQuery(sql);
			while (rs.next()) {
				System.out.println(rs.getString(1));
				dbLst.add(rs.getString(1));
			}
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

		return dbLst;
	}

	public HashMap<String, ArrayList> getTableSchema(Connection c) {
		HashMap<String, ArrayList> tableMap = new HashMap<String, ArrayList>();
		ResultSet tableSet;

		try {
			DatabaseMetaData dbmd = c.getMetaData();

			ResultSet primaryKeySet;
			ResultSet resultSet = dbmd.getTables(null, "%", "%", new String[] { "TABLE" });
			while (resultSet.next()) {
				String tableName = resultSet.getString("TABLE_NAME");

				ArrayList<String> attLst = new ArrayList<>();
				tableSet = dbmd.getColumns(null, null, tableName, "%");

				while (tableSet.next()) {
					attLst.add(tableSet.getString(4));
				}
				tableMap.put(tableName, attLst);
			}

			System.out.println("schema:\t" + tableMap);

		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return tableMap;
	}

	/**
	 * get all the table names in the database
	 */
	public ArrayList<String> getTableNames(Connection conn) {
		ArrayList<String> tableNames = new ArrayList<>();
		ResultSet rs = null;
		try {
			// Get the metadata of the database
			DatabaseMetaData db = conn.getMetaData();
			// Get all table names from metadata
			rs = db.getTables(null, null, null, new String[] { "TABLE" });
			while (rs.next()) {
				tableNames.add(rs.getString(3));
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("getTableNames failure: " + e.getMessage());
			System.exit(0);
		}
		return tableNames;
	}

	/**
	 * get the name of column
	 * 
	 * @param tableName
	 * 
	 * @return
	 */
	public ArrayList<String> getColumnNames(String tableName, Connection conn) {
		ArrayList<String> columnNames = new ArrayList<>();

		PreparedStatement pStemt = null;
		String tableSql = SQL + tableName;
		try {
			pStemt = conn.prepareStatement(tableSql);

			ResultSetMetaData rsmd = pStemt.getMetaData();
			// number of columns
			int size = rsmd.getColumnCount();
			for (int i = 0; i < size; i++) {
				columnNames.add(rsmd.getColumnName(i + 1));
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("getColumnNames failure: " + e.getMessage());
			System.exit(0);
		}
		return columnNames;
	}

	/**
	 * get type of column
	 * 
	 * @param tableName
	 * @return
	 */
	public ArrayList<String> getColumnTypes(String tableName, Connection conn) {
		ArrayList<String> columnTypes = new ArrayList<>();

		PreparedStatement pStemt = null;
		String tableSql = SQL + tableName;
		try {
			pStemt = conn.prepareStatement(tableSql);
			// Get the metadata of the database
			ResultSetMetaData rsmd = pStemt.getMetaData();
			// number of clumn in table
			int size = rsmd.getColumnCount();
			for (int i = 0; i < size; i++) {

				columnTypes.add(rsmd.getColumnTypeName(i + 1));

			}
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("getColumnTypes failure: " + e.getMessage());
			System.exit(0);
		}
		return columnTypes;
	}


	/**
	 * Get primary key
	 * 
	 * @throws SQLException
	 */
	public ArrayList<String> getPKey(String tableName, Connection conn) throws SQLException {
		ArrayList<String> Pkey = new ArrayList<>();
		// Get the metadata of the database
		DatabaseMetaData db = conn.getMetaData();
		ResultSet pkRSet = db.getPrimaryKeys(null, null, tableName);

		// get all the primary key of the table
		while (pkRSet.next()) {
			Pkey.add(pkRSet.getString("COLUMN_NAME"));
		}
		return Pkey;
	}
	/*
	 * public void main(String[] args) { Connection conn = connectDB();
	 * ArrayList<String> tableNames = getTableNames(conn);
	 * System.out.println("tableNames:" + tableNames); for (String tableName :
	 * tableNames) { System.out.println("ColumnNames:" + getColumnNames(tableName,
	 * conn)); System.out.println("ColumnTypes:" + getColumnTypes(tableName, conn));
	 * System.out.println("ColumnComments:" + getColumnComments(tableName)); } }
	 */

	/**
	 * Insert data into database
	 * 
	 * @param conn
	 * @param tableName
	 * @param data
	 */
	public void insertdata(Connection conn, String tableName, String data) {

		try {
			// generate insert sql

			String sql = "insert into " + tableName + " VALUES ( " + data + ")";

			// operate sql

			PreparedStatement psql = conn.prepareStatement(sql);
			psql.executeUpdate(); // execute sql
			psql.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// System.out.println("Insert successfully！" + "\n");
		}
	}

	public void insertBanchdata(Connection conn, HashMap<String, String> data) {
		try {
			conn.setAutoCommit(false);
			Statement stmt = null;
			stmt = conn.createStatement();
			int i = 0;
			for (Map.Entry<String, String> entry : data.entrySet()) {
				String sql = "insert into " + entry.getValue() + " VALUES ( " + entry.getKey() + ")";

				stmt.addBatch(sql);
				i++;
				if(i%1000==0){//可以设置不同的大小；如50，100，500，1000等等
					stmt.executeBatch();
					conn.commit();
					stmt.clearBatch();
				}
			}
			stmt.executeBatch();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	/**
	 * update data
	 * 
	 * @param conn
	 */
	public static void updatedata(Connection conn) {
		try {
			PreparedStatement psql;
			psql = conn.prepareStatement("update emp set sal = ? where ename = ?");
			psql.setFloat(1, (float) 5000.0);
			psql.setString(2, "Mark");
			psql.executeUpdate();
			psql.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("Modefy successfully！" + "\n");
		}
	}

	/**
	 * delete data
	 * 
	 * @param conn
	 */
	public static void deletedata(Connection conn) {
		try {
			PreparedStatement psql;
			psql = conn.prepareStatement("delete from emp where sal < ?");
			psql.setFloat(1, 3000.00F);
			psql.executeUpdate();
			psql.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			System.out.println("Delete successfully！" + "\n");
		}

	}

	public void executeSQL(String sql, Connection conn) {

		PreparedStatement psql;
		try {
			psql = conn.prepareStatement(sql);
			psql.executeUpdate(); // execute sql
			psql.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public boolean validateTableNameExist(String tableName, Connection conn) {
		try {

			ResultSet rs = conn.getMetaData().getTables(null, null, tableName, null);
			if (rs.next()) {
				return true;
			} else {
				return false;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return false;
	}

}
