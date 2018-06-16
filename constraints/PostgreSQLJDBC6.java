import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;

public class PostgreSQLJDBC6 {
    public String dbName = "temp";
    public String port = "5432";
    public String usrName = "postgres";
    public String psw = "123";


    public Connection connectDB() {
        Connection c = null;
        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager.getConnection("jdbc:postgresql://localhost:" + port + "/" + dbName, usrName, psw);
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
}