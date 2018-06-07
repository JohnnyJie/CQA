import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/*
Version 1:
add sql statement execute

*/

public class PostgreSQLJDBC {
	public String dbName = "cqa";
	public String port = "5432"; 
	public String usrName = "postgres";
	public String psw = "123";
	public String sqlPath = "/Users/johnny/workplace/MSCProj/library_test_data2.sql";

	public static void main(String args[]) {
		PostgreSQLJDBC po = new PostgreSQLJDBC();
		po.execute();
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

	public void execute() {
		Connection c = connectDB();
		Statement stmt = null;
		try {
			String path = sqlPath;
	        String sqlTest = getText(path);
	        List<String> sqlarr = getSql(sqlTest);
	        
			stmt = c.createStatement();
			for (String sql : sqlarr) {
				sql = sql.trim();
				System.out.println(sql);
				if (sql != null && !sql.equals("")) {
//					c.addBatch(sql);
                    ResultSet rs = stmt.executeQuery(sql);
//                    while (rs.next()) {
//                        System.out.println(rs.getString("title"));
//
//                    }
                }
			}


			stmt.close();
			c.close();
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
		System.out.println("Successful");
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
