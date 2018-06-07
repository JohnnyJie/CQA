import javafx.beans.binding.StringBinding;

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

Version 5:
auto primary key checking
 */

public class PostgreSQLJDBC5 {
	public String dbName = "temp";
	public String port = "5432";
	public String usrName = "postgres";
	public String psw = "123";
	public String originQueryPath = "/Users/johnny/workplace/MSCProj/originQuery.sql";
	public String constraintsPath = "/Users/johnny/workplace/MSCProj/pkeyConstraints";


	public static void main(String args[]) {
		PostgreSQLJDBC5 po = new PostgreSQLJDBC5();
		po.sampleFramework();
	}

	public void sampleFramework(){
		float epsilon = 0.1f; //why??
		float theta = 0.01f;
		int count = 0 ;
		Random random = new Random();
		int m = (int)((1/(2*epsilon))* Math.log(2/theta));
		System.out.println("m: "+m);

		PostgreSQLJDBC5 po = new PostgreSQLJDBC5();
		try {
			Connection c = connectDB();
			HashMap tableMap = po.getTableSchema(c);  //get schema
			ArrayList<ConstraintStru> constraintArrayLst = po.getPKeyDepStruLst(c,tableMap);
			HashMap<String,HashMap<String,ArrayList>> tableVioMap = po.violationCheck(c,constraintArrayLst,tableMap); // check violation for each table with constraints
			Map<String,String> createTableSql = createIARTableSql(constraintArrayLst,tableMap); // IAR semantics for each table
			for(Object entry :createTableSql.entrySet()){
				String sql = (String) ((Map.Entry)entry).getValue();
				po.execute(c,sql,false);
			}

			//Run Row(SQL(theta)) for each theta
			for(ConstraintStru constraintStru: constraintArrayLst){
				String tableName = constraintStru.getpKeyDepStru().getTableName();

				for(int i=0;i<=m;i++){
					String createTempSql = "CREATE TABLE tmp_" +  tableName + " as SELECT * FROM old_" + tableName +" ;";
					po.execute(c,createTempSql,false);
					po.repair(c, random,tableVioMap,(ArrayList)tableMap.get(tableName),constraintStru);  //choose one tuple add to the oldTable
					HashMap<String,HashMap<String,ArrayList>> subTableVioMap = po.violationCheck(c,constraintStru,tableMap,"tmp_" + tableName); //check for single theta
					while(!subTableVioMap.isEmpty()){
						//delete
						for (Object entry : subTableVioMap.entrySet()) {
							//String pKeyCombination = (String) ((Map.Entry)entry).getKey();
							HashMap<String,ArrayList> subVioTupleMapLst = (HashMap) ((Map.Entry)entry).getValue();
							for (Object subEntry : subVioTupleMapLst.entrySet()) {
								ArrayList<HashMap> subTupleAttMapLst = (ArrayList) (((Map.Entry)subEntry).getValue());
								for(HashMap subTupleMap: subTupleAttMapLst) {
									String sql = "DELETE FROM tmp_" + tableName + " WHERE ";
									for (Object tupleEntry : subTupleMap.entrySet()) {
										sql += (((Map.Entry) tupleEntry).getKey()) + " = '" + (((Map.Entry) tupleEntry).getValue()) + "' AND ";
									}
									sql = sql.substring(0, sql.length() - 4); //remove the last "AND"
									sql += ";";
									this.execute(c, sql, false);
								}
							}
						}
						//repair
						po.repair(c, random,subTableVioMap,(ArrayList<String>) tableMap.get(tableName),constraintStru);
						subTableVioMap = po.violationCheck(c,constraintStru,tableMap,"tmp_" + tableName);
					}

					Boolean bool = po.executeFile(c ,po.originQueryPath,true);
					if(bool) {
						count++;
					}
					System.out.println("---------------------------------------------------------------------: " + i);
					String sql = "DROP TABLE tmp_" + tableName + ";";
					this.execute(c, sql,false);

				}

			}

			for(Object entry :createTableSql.entrySet()){
				String table = (String) ((Map.Entry)entry).getKey();
				String sql = "DROP TABLE old_" + table;
				po.execute(c,sql,false);
			}


			c.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(count + "/" + m);
	}

	public void repair(Connection c,Random random,HashMap<String,HashMap<String,ArrayList>> tableVioMap,ArrayList<String> attLst ,ConstraintStru constraintStru){

		String tableName = constraintStru.getpKeyDepStru().getTableName();
		String sql = "INSERT INTO tmp_" + tableName + "( ";
		for (String attName : attLst) {
			sql += attName + ",";
		}
		sql = sql.substring(0,sql.length()-1); //remove the last ","
		sql += ") VALUES ";
		//String sql = "INSERT INTO tmp_table(rid,bid) " ;
		for (Object entry : tableVioMap.get(tableName).entrySet()) {
			ArrayList<HashMap<String,String>> tupleAttMapLst = (ArrayList<HashMap<String,String>>) ((Map.Entry)entry).getValue();
			int index = Math.abs(random.nextInt()) % tupleAttMapLst.size();
			HashMap<String,String> tupleAttMap = tupleAttMapLst.get(index);
			sql += " ( ";
			for (String attName :attLst) {
				sql += "'" + tupleAttMap.get(attName) + "',";
			}
			sql = sql.substring(0,sql.length()-1); //remove the last ","
			sql += " ),";
		}
		sql = sql.substring(0,sql.length()-1);//remove the last ","
		sql += ";";
		try {
			this.execute(c, sql,false);
		} catch (SQLException e) {

			try {
				String error_sql = "DROP TABLE tmp_" + tableName + ";";
				this.execute(c, error_sql,false);
				error_sql = "DROP TABLE old_" + tableName + ";";
				this.execute(c, error_sql,false);
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
		}

	}

	public HashMap<String,HashMap<String,ArrayList>> violationCheck(Connection c,ConstraintStru constraint,HashMap tableMap,String tempTableName) {
		Statement stmt = null;

		HashMap<String,HashMap<String,ArrayList>> tableVioMap = new HashMap();

		try {

			stmt = c.createStatement();
			HashMap vioTupleMap = new HashMap();
			PKeyDepStru pKeyDepStru = constraint.getpKeyDepStru();
			String sql = constraint.getDepSql();
			ArrayList<String> attLst = (ArrayList)tableMap.get(pKeyDepStru.getTableName());
			ArrayList<String> pkeyList = pKeyDepStru.getPkeyList();

			sql = sql.trim();
			System.out.println(sql);
			int count = 0 ;
			if (sql != null && !sql.equals("")) {
//					c.addBatch(sql);
				sql = sql.replaceAll(constraint.getpKeyDepStru().getTableName(),tempTableName); //replace to the temp tableName
				ResultSet rs = stmt.executeQuery(sql);
				while (rs.next()) {
					String pKeyCombine = "";
					for(String pKey: pkeyList){
						pKeyCombine += rs.getString(pKey) + ",";
					}
					HashMap tupleAttMap = new HashMap();
					for (String attName : attLst){
						tupleAttMap.put(attName,rs.getString(attName));
					}


					//System.out.println(rs.getString(1)+"\t"+rs.getString(2));
					if (vioTupleMap.get(pKeyCombine) == null){
						ArrayList<HashMap> pKeyCombineLst = new ArrayList<>();
						pKeyCombineLst.add(tupleAttMap);
						vioTupleMap.put(pKeyCombine,pKeyCombineLst);
					}else{
						ArrayList<HashMap> pKeyCombineLst = (ArrayList)vioTupleMap.get(pKeyCombine);
						pKeyCombineLst.add(tupleAttMap);
						vioTupleMap.put(pKeyCombine,pKeyCombineLst);
					}
				}
			}
			if(!vioTupleMap.isEmpty())
				tableVioMap.put(pKeyDepStru.getTableName(),vioTupleMap);
			System.out.println(vioTupleMap + "  =============");


			stmt.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(e.getClass().getName() + ": " + e.getMessage());

		}
		System.out.println("Successful");
		return tableVioMap;
	}

	public HashMap violationCheck(Connection c,ArrayList<ConstraintStru> constraintStruLst,Map tableMap) {
		Statement stmt = null;

		HashMap tableVioMap = new HashMap();

		try {

			stmt = c.createStatement();
			for (ConstraintStru constraint : constraintStruLst) {
				Map vioTupleMap = new HashMap();
				PKeyDepStru pKeyDepStru = constraint.getpKeyDepStru();
				String sql = constraint.getDepSql();
				ArrayList<String> attMap = (ArrayList)tableMap.get(pKeyDepStru.getTableName());
				ArrayList<String> pkeyList = pKeyDepStru.getPkeyList();

				sql = sql.trim();
				System.out.println(sql);
				int count = 0 ;
				if (sql != null && !sql.equals("")) {
//					c.addBatch(sql);
					ResultSet rs = stmt.executeQuery(sql);
					while (rs.next()) {
						String pKeyCombine = "";
						for(String pKey: pkeyList){
							pKeyCombine += rs.getString(pKey) + ",";
						}
						HashMap tupleAttMap = new HashMap();
						for(String attname : attMap){
							tupleAttMap.put(attname,rs.getString(attname));
						}


						//System.out.println(rs.getString(1)+"\t"+rs.getString(2));
						if (vioTupleMap.get(pKeyCombine) == null){
							ArrayList<HashMap> pKeyCombineLst = new ArrayList<>();
							pKeyCombineLst.add(tupleAttMap);
							vioTupleMap.put(pKeyCombine,pKeyCombineLst);
						}else{
							ArrayList<HashMap> pKeyCombineLst = (ArrayList)vioTupleMap.get(pKeyCombine);
							pKeyCombineLst.add(tupleAttMap);
							vioTupleMap.put(pKeyCombine,pKeyCombineLst);
						}
					}
				}
				tableVioMap.put(pKeyDepStru.getTableName(),vioTupleMap);
				System.out.println(vioTupleMap + "  =============");
			}
			stmt.close();
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());

		}
		System.out.println("Successful");
		return tableVioMap;
	}

	public ArrayList<ConstraintStru> getPKeyDepStruLst(Connection c,HashMap tableMap){
		/*
		1st: table name
		2nd: read the database schema
		3rd: sql produce
		 */

		SQLAnalyzer analyzer = new SQLAnalyzer();
		String sentence = this.getText(constraintsPath);

		ArrayList<PKeyDepStru> pKeyDepStruLst = analyzer.tokenise(sentence);

		ArrayList<ConstraintStru> constraintStruLst = new ArrayList<>();


		for(PKeyDepStru pKeyDepStru: pKeyDepStruLst){

			String sql = "SELECT DISTINCT ";
			for(String attName : (ArrayList<String>)tableMap.get(pKeyDepStru.getTableName())){
				sql += "A." + attName + " ,";
			}
			sql = sql.substring(0,sql.length()-1); //remove the last ","
			sql += "  From ";
			String tableName = pKeyDepStru.getTableName();
			sql += tableName + " as A, " + tableName +" as B WHERE ";
			ArrayList tableAttLst = new ArrayList();
			tableAttLst = (ArrayList) ((ArrayList)tableMap.get(tableName)).clone();
			ArrayList pKeyLst = pKeyDepStru.getPkeyList();
			for(Object pKey: pKeyLst){
				String key = (String)pKey;
				sql += "A." + key + " = B." + key + " AND ";
				tableAttLst.remove(key);
			}

			sql += "( ";
			int count = 1;
			for(Object tableAtt: tableAttLst){
				String att = (String)tableAtt;
				sql += "A." + att + " <> B." + att;
				if (count == tableAttLst.size()){
					sql += ");";
				}else {
					sql += " OR ";
					count ++;
				}
			}
			// save the rewrite sql with constraint
			ConstraintStru constraintStru = new ConstraintStru(pKeyDepStru,sql);
			constraintStruLst.add(constraintStru);
		}

		return constraintStruLst;

	}

	public HashMap<String,String> createIARTableSql(ArrayList<ConstraintStru> constraintArrayLst, HashMap tableMap){
		HashMap<String,String> IARTableSql = new HashMap<>();
		for(ConstraintStru constraintStru: constraintArrayLst) {
			String sql = "CREATE TABLE old_" +constraintStru.getpKeyDepStru().getTableName() +" as SELECT * FROM " + constraintStru.getpKeyDepStru().getTableName()
					+ " WHERE ( ";
			ArrayList<String> attMap = (ArrayList)tableMap.get(constraintStru.getpKeyDepStru().getTableName());
//			for (int i = 0; i < getPkeyList.size(); i++) {
//
//				sql += getPkeyList.get(i) + " ";
//				if (i + 1 < getPkeyList.size()) {
//					sql += ", ";
//				}
//			}
			for(String attname : attMap){
				sql += attname + ",";
			}
			sql = sql.substring(0,sql.length()-1); //remove the last ","
			String depSql = constraintStru.getDepSql();
			depSql = depSql.substring(0,depSql.length()-1); //remove the last ";"
			sql += ") NOT IN ( " + depSql + " );";
			IARTableSql.put(constraintStru.getpKeyDepStru().getTableName(),sql);
		}
		return IARTableSql;
	}

	public HashMap getTableSchema(Connection c){
		HashMap tableMap = new HashMap();
		ResultSet tableSet;

		try {
			DatabaseMetaData dbmd=c.getMetaData();


			ResultSet primaryKeySet;
			ResultSet resultSet = dbmd.getTables(null, "%", "%", new String[] { "TABLE" });
			while (resultSet.next()) {
				String tableName =resultSet.getString("TABLE_NAME");

				ArrayList<String> attLst = new ArrayList<>();
				tableSet = dbmd.getColumns(null, null, tableName, "%");

				while (tableSet.next()) {
					attLst.add(tableSet.getString(4));
				}
				tableMap.put(tableName,attLst);
			}

			System.out.println("schema:\t" + tableMap);


		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

		return tableMap;
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
