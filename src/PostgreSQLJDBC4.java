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

Version 4:
code optimization
sample specialize framework
 */

public class PostgreSQLJDBC4 {
	public String dbName = "cqa";
	public String port = "5432";
	public String usrName = "postgres";
	public String psw = "123";
	public String oldTableSqlPath = "/Users/johnny/workplace/MSCProj/oldTable.sql";
	public String pasteOldTableSqlPath = "/Users/johnny/workplace/MSCProj/pasteOldTable.sql";
	public String vioCheckBorrowSqlPath = "/Users/johnny/workplace/MSCProj/vioCheckBorrow.sql";
	public String vioCheckOldTable = "/Users/johnny/workplace/MSCProj/vioCheckOldTable.sql";
	public String originQueryPath = "/Users/johnny/workplace/MSCProj/originQuery.sql";
	private Map tableMap;

	public static void main(String args[]) {
		/*{
			PostgreSQLJDBC4 po = new PostgreSQLJDBC4();
			po.tableMap = po.getTableSchema();  //get schema
			Map tupleMap = po.violationCheck(po.vioCheckBorrowSqlPath);
			try {
				File file = new File(po.oldTableSqlPath);
				ArrayList<ResultSet> resultSets = po.execute(file,false); //process sql file
			} catch (SQLException e) {
				e.printStackTrace();
			}
			Random random = new Random();
			po.repair(random.nextInt(),tupleMap);
		}*/
		PostgreSQLJDBC4 po = new PostgreSQLJDBC4();
		po.sampleFramework();
	}


	public void sampleFramework(){
		float epsilon = 0.1f;
		float theta = 0.01f;
		int count = 0 ;
		Random random = new Random();
		int m = (int)((1/(2*epsilon))* Math.log(2/theta));
		System.out.println("m: "+m);

		PostgreSQLJDBC4 po = new PostgreSQLJDBC4();
		try {
			Connection c = connectDB();

			//po.tableMap = po.getTableSchema();  //get schema
			Map tupleMap = po.violationCheck(c,po.vioCheckBorrowSqlPath);
			po.executeFile(c,po.oldTableSqlPath,false); // create oldTable

			for(int i=0;i<=m;i++){
				po.executeFile(c,po.pasteOldTableSqlPath,false);// paste old_table
				po.repair(c, random.nextInt(),tupleMap);
				Map vioTupleMap = po.violationCheck(c,po.vioCheckOldTable);
				while(!vioTupleMap.isEmpty()){
					//delete
					for (Object entry : vioTupleMap.entrySet()) {
						String rid = (String) ((Map.Entry)entry).getKey();
						ArrayList bidLst = (ArrayList) ((Map.Entry)entry).getValue();
						for(Object bid: bidLst){
							String sql = "DELETE FROM tmp_table WHERE rid = '" + rid +"' AND bid = '" + (String)bid  + "'";
							this.execute(c, sql,false);
						}
					}
					//repair
					po.repair(c, random.nextInt(),vioTupleMap);
					vioTupleMap = po.violationCheck(c, po.vioCheckOldTable);
				}

				Boolean bool = po.executeFile(c ,po.originQueryPath,true);
				if(bool) {
					count++;
				}
				System.out.println("---------------------------------------------------------------------: " + i);
				String sql = "DROP TABLE tmp_table";
				this.execute(c, sql,false);

			}
			String sql = "DROP TABLE oldTable";
			this.execute(c, sql,false);
			c.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(count + "/" + m);
	}


	public void repair(Connection c,int randomInt,Map tupleMap){
		int i = 0;
		String sql = "INSERT INTO tmp_table(rid,bid) " ;
		for (Object entry : tupleMap.entrySet()) {
			String bid = (String) ((Map.Entry)entry).getKey();
			ArrayList ridLst = (ArrayList) ((Map.Entry)entry).getValue();
			int index = Math.abs(randomInt) % ridLst.size();
//			System.out.println(index);
			sql += "SELECT " + ridLst.get(index) + "," + bid + " ";
			i ++;
			if(i == tupleMap.entrySet().size() - 1){
				sql += ";";
				try {
					this.execute(c, sql,false);
				} catch (SQLException e) {

					try {
						String error_sql = "DROP TABLE tmp_table";
						this.execute(c, error_sql,false);
						error_sql = "DROP TABLE oldTable";
						this.execute(c, error_sql,false);
					} catch (SQLException e1) {
						e1.printStackTrace();
					}
					e.printStackTrace();
				}
			}else {
				sql += " UNION ALL ";
			}
		}

	}

	public Map violationCheck(Connection c,String path) {
		Statement stmt = null;
		Map vioTupleMap = new HashMap();

		try {

			File file = new File(path);
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
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());

		}
		System.out.println("Successful");
		return vioTupleMap;
	}


	private Map getTableSchema(Connection c){
		try {
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

	public void execute(Connection c,String sql,Boolean isQuery) throws SQLException {
		Statement stmt = null;
		ResultSet rs = null;
		try {
			stmt = c.createStatement();
			sql = sql.trim();
			System.out.println(sql);
			if (sql != null && !sql.equals("")) {
				if (isQuery) {
					rs = stmt.executeQuery(sql);
					while (rs.next()) {
						//                        System.out.println(rs.getString(1)+"\t"+rs.getString(2)+"\t"+rs.getString(3));
						//                        System.out.println(rs.getString(1));
					}
				}else{
					stmt.execute(sql);
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
		}
	}

	public Boolean executeFile(Connection c,String path,Boolean isQuery) throws SQLException {
		Statement stmt = null;
		Boolean bool = false;
		ResultSet rs = null;

		try {
			String sqlTest = getText(path);
			List<String> sqlarr = getSql(sqlTest);
			stmt = c.createStatement();
			for (String sql : sqlarr) {
				sql = sql.trim();
				System.out.println(sql);
				int count = 0 ;
				if (sql != null && !sql.equals("")) {
					if (isQuery){
						rs = stmt.executeQuery(sql);
						while (rs.next()) {
							bool = true;
                        	System.out.println(rs.getString(1)+"\t"+rs.getString(2));
//                        System.out.println(rs.getString(1));
							count++;
						}
					}else{
						stmt.execute(sql);
					}
				}
				System.out.println(count + "  =============");
			}
			stmt.close();
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
		}
		return bool;
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
