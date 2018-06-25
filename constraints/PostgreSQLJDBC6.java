import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

public class PostgreSQLJDBC6 {


    public Connection connectDB(String address,String port,String dbName, String usrName, String psw ) {
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

    public HashMap getTableSchema(Connection c) {
        HashMap<String,ArrayList> tableMap = new HashMap();
        ResultSet tableSet;

        try {
            DatabaseMetaData dbmd = c.getMetaData();


            ResultSet primaryKeySet;
            ResultSet resultSet = dbmd.getTables(null, "%", "%", new String[]{"TABLE"});
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

    public ArrayList<String> listDbName(Connection c){
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

        return  dbLst;
    }


    public ResultSet execute(Connection c,String sql,Boolean isQuery) throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = c.createStatement();
            sql = sql.trim();
            System.out.println(sql);
            if (sql != null && !sql.equals("")) {
                if (isQuery) {
                    rs = stmt.executeQuery(sql);
                    return rs;
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
        return null;
    }

}