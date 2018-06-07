import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.*;


/*
Version 1:
add sql statement execute

Version 2:
add database schema extract

Version 3:
add violation check
add repair

 */
public class PostgreSQLJDBC3 {
	public String dbName = "cqa";
	public String port = "5432";
	public String usrName = "postgres";
	public String psw = "123";
	public String sqlPath = "/Users/johnny/workplace/MSCProj/test2.sql";
	public String vioCheckSqlPath = "/Users/johnny/workplace/MSCProj/test.sql";

	private Map tableMap;
	private Map tupleMap;

	public static void main(String args[]) {
		PostgreSQLJDBC3 po = new PostgreSQLJDBC3();
		po.tableMap = po.getTableSchema();  //得到关系模式
		po.tupleMap = po.violationCheck();
		try {
			po.execute(); //执行sql文件
		} catch (SQLException e) {
			e.printStackTrace();
		}
		po.repair();
	}


	public void repair(){
		for (Object entry : this.tupleMap.entrySet()) {
			String rid = (String) ((Map.Entry)entry).getKey();
			ArrayList bidLst = (ArrayList) ((Map.Entry)entry).getValue();
			Random random = new Random();
			int index = Math.abs(random.nextInt()) % bidLst.size();
			System.out.println(index);
			String sql = "INSERT INTO oldtable (rid,bid) VALUES('" + rid +"','" + bidLst.get(index) + "')";
			try {
				this.execute(sql);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public Map violationCheck() {
		Connection c = connectDB();
		Statement stmt = null;
		Map vioTupleMap = new HashMap();

		try {
			String path = vioCheckSqlPath;
			String sqlTest = getText(path);

			List<String> sqlarr = getSql(sqlTest);
			stmt = c.createStatement();
			for (String sql : sqlarr) {
				sql = sql.trim();
				System.out.println(sql);
				int count = 0 ;
				if (sql != null && !sql.equals("")) {
//					c.addBatch(sql);
					ResultSet rs = stmt.executeQuery(sql);
					while (rs.next()) {
						String rid = rs.getString(1);
						String bid = rs.getString(2);
						//System.out.println(rs.getString(1)+"\t"+rs.getString(2));
						if (vioTupleMap.get(bid) == null){
							ArrayList<String> ridLst = new ArrayList<>();
							ridLst.add(rid);
							vioTupleMap.put(bid,ridLst);
						}else{
							ArrayList<String> ridLst = (ArrayList)vioTupleMap.get(bid);
							ridLst.add(rid);
							vioTupleMap.put(bid,ridLst);
						}
					}
				}
				System.out.println(vioTupleMap + "  =============");
			}
			stmt.close();
			c.close();
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
		System.out.println("Successful");
		return vioTupleMap;
	}


	private Map getTableSchema(){
		try {
			Connection c = connectDB();
			DatabaseMetaData dbmd=c.getMetaData();
			Map tableMap = new HashMap();
			ResultSet tableSet;
			ResultSet primaryKeySet;
			ResultSet resultSet = dbmd.getTables(null, "%", "%", new String[] { "TABLE" });

			while (resultSet.next()) {
				String tableName =resultSet.getString("TABLE_NAME");

				System.out.println(tableName+"===============");

				Map attMap = new HashMap();

				primaryKeySet = dbmd.getPrimaryKeys(null, null, tableName);
				while (primaryKeySet.next()) {
					System.out.println(primaryKeySet.getString(3) + "表的主键是：" + primaryKeySet.getString(4));
				}

				tableSet = dbmd.getColumns(null, null, tableName, "%");


				while (tableSet.next()) {
					attMap.put(tableSet.getString(4),tableSet.getString(6));
					System.out.println(tableSet.getString(4) + " " + tableSet.getString(6) + "(" + tableSet.getString(7) + ");");
				}
				tableMap.put(tableName,attMap);
			}

			System.out.println("schema:\t" + tableMap);

			return tableMap;

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public void execute(String sql) throws SQLException {
		Connection c = connectDB();
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = c.createStatement();
			sql = sql.trim();
			System.out.println(sql);
			if (sql != null && !sql.equals("")) {
//					c.addBatch(sql);
				rs = stmt.executeQuery(sql);

				while (rs.next()) {
//                        System.out.println(rs.getString(1)+"\t"+rs.getString(2)+"\t"+rs.getString(3));
//                        System.out.println(rs.getString(1));
				}
			}

		} catch (Exception e) {

			System.err.println(e.getClass().getName() + ": " + e.getMessage());

		} finally {
			if (rs != null) {
				try {
					rs.close();
					rs = null;
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
					stmt = null;
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			c.close();
			// closeConnection();
		}
	}

	public void execute() throws SQLException {
		Connection c = connectDB();
		Statement stmt = null;
		ResultSet rs = null;
		try {
			String path = sqlPath;
			String sqlTest = getText(path);
			List<String> sqlarr = getSql(sqlTest);
			stmt = c.createStatement();
			for (String sql : sqlarr) {
				sql = sql.trim();
				System.out.println(sql);
				int count = 0 ;
				if (sql != null && !sql.equals("")) {
					rs = stmt.executeQuery(sql);

					while (rs.next()) {
//                        System.out.println(rs.getString(1)+"\t"+rs.getString(2)+"\t"+rs.getString(3));
//                        System.out.println(rs.getString(1));
						count++;
					}
				}
				System.out.println(count + "  =============");
			}
			stmt.close();
			c.close();
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());

		}finally {
			if (rs != null) {
				try {
					rs.close();
					rs = null;
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
					stmt = null;
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			c.close();
		}
	}

	public Connection connectDB() {
		Connection c = null;
		try {
			Class.forName("org.postgresql.Driver");
			c = DriverManager.getConnection("jdbc:postgresql://localhost:"+port+"/"+ dbName, usrName, psw);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
		System.out.println("Opened database successfully");
		return c;
	}

	public List<String> getSql(String sql) {
		String s = sql;
		s = s.replaceAll("\r\n", "\r");
		s = s.replaceAll("\r", "\n");
		List<String> ret = new ArrayList<String>();
		String[] sqlarry = s.split(";");
		sqlarry = filter(sqlarry);
		ret = Arrays.asList(sqlarry);
		return ret;
	}

	public String getText(String path){
		File file = new File(path);
		if(!file.exists()||file.isDirectory()){
			return null;
		}
		StringBuilder sb = new StringBuilder();
		try{
			FileInputStream fis = new FileInputStream(path);
			InputStreamReader isr = new InputStreamReader(fis,"UTF-8");
			BufferedReader br = new BufferedReader(isr);
			String temp = null;
			temp = br.readLine();
			while(temp!=null){
				if(temp.length()>=2){
					String str1 = temp.substring(0, 1);
					String str2 = temp.substring(0, 2);
					if(str1.equals("#")||str2.equals("--")||str2.equals("/*")||str2.equals("//")){
						temp = br.readLine();
						continue;
					}
					sb.append(temp+"\r\n");
				}

				temp = br.readLine();
			}
			br.close();

		}catch(Exception e){
			e.printStackTrace();
		}
		return sb.toString();
	}

	public String[] filter(String[] ss) {
		List<String> strs = new ArrayList<String>();
		for (String s : ss) {
			if (s != null && !s.equals("")) {
				strs.add(s);
			}
		}
		String[] result = new String[strs.size()];
		for (int i = 0; i < strs.size(); i++) {
			result[i] = strs.get(i).toString();
		}
		return result;
	}
}
