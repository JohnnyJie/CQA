import javax.swing.*;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Main {
    private JComboBox dbBox;
    private JTextField addrText;
    private JTextField portText;
    private JLabel dbLabel;
    private JLabel addrLabel;
    private JLabel portLabel;
    private JLabel userLabel;
    private JTextField userText;
    private JLabel psdLabel;
    private JTextField psdText;
    private JLabel consLabel;
    private JTextArea consText;
    private JLabel queryLabel;
    private JTextArea queryText;
    private JLabel errorLabel;
    private JTextArea outputText;
    private JPanel outputPanel;
    private JPanel inputPanel;
    private JPanel configPanel;
    private JPanel MainPanel;
    private JButton freshBtn;
    private JButton queryBtn;
    private JScrollPane ouputScroll;
    private JTextField errorText;
    private JLabel confidenceLabel;
    private JTextField confidenceText;
    private JPanel controlPanel;

    private String address = "localhost";
    private String dbName = "temp";
    private String port = "5432";
    private String usrName = "postgres";
    private String psw = "123";
    private String constraints = "reader(a,b,c,d,e,f),reader'(g,h,c,i,j,k) -: [ false |a=g]";
    private float epsilon = 0.1f;
    private float theta = 0.01f;
    private static Connection c;
    private PostgreSQLJDBC6 postgreSQLJDBC;
    private HashMap<String,ArrayList> tableMap;
    private ArrayList<ConstraintStru2> constraintStruLst;

    public static void main(String[] args) {
        Main main = new Main();
        JFrame frame = new JFrame("Main");
        frame.setContentPane(main.MainPanel);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
        frame.addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                super.windowClosing(e);
                try {
                    // remove the deletion table
                    if(main.constraintStruLst != null){
                        for(ConstraintStru2 constraintStru: main.constraintStruLst){
                            int sequence = constraintStru.getSequence();
                            String error_sql = "DROP TABLE delTable"  + sequence + ";";
                            main.postgreSQLJDBC.execute(c, error_sql,false);
                        }
                    }

                    c.close();
                    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
        });
        main.initGUI();  // init the components
    }

    public Main() {
        postgreSQLJDBC = new PostgreSQLJDBC6();
        c = postgreSQLJDBC.connectDB(address, port,dbName,usrName,psw); // init the connection of the database
        tableMap = postgreSQLJDBC.getTableSchema(c);  // the schema of the database

    }

    public void initGUI(){
        addrText.setText(address);
        portText.setText(port);
        userText.setText(usrName);
        psdText.setText(psw);
        consText.setText(constraints);
        errorText.setText(String.valueOf(epsilon));
        confidenceText.setText(String.valueOf(theta));
        ArrayList<String> dbLst = postgreSQLJDBC.listDbName(c);
        for (String name : dbLst){
            dbBox.addItem(name);
        }

        // get the new user input parameter and create new connection
        freshBtn.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                super.mouseClicked(e);
                address = addrText.getText().trim();
                port = portText.getText().trim();
                dbName = dbBox.getSelectedItem().toString().trim();
                usrName = userText.getText().trim();
                psw = psdText.getText().trim();
                c = postgreSQLJDBC.connectDB(address, port,dbName,usrName,psw);
                tableMap = postgreSQLJDBC.getTableSchema(c);
            }
        });
        // execute query button
        queryBtn.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                super.mouseClicked(e);
                constraintStruLst = new ArrayList<>();
                // invoke the query analysis to get the regarding query tables




                epsilon = Float.valueOf(errorText.getText().trim());  // error bound set by user
                theta = Float.valueOf(confidenceText.getText().trim());  // confidence set by user

                try {
                    int sequence = 0;  // record which constraint
                    for (String constraint : consText.getText().split(";")) {
                        ConstraintStru2 constraintStru= violationCheck(constraint.trim(),sequence);
                        constraintStru.setSequence(sequence);
                        constraintStruLst.add(constraintStru);  // add violation tuples with regarding sql
                    }
                    sampleFramework(constraintStruLst);
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
        });

    }

    /********
     *
     * @param constraint
     * @param
     * @return
     * @throws SQLException
     *  find the violation tuples and regarding table, and save them in the del_ table
     */
    public ConstraintStru2 violationCheck(String constraint, int sequence) throws SQLException {

        ConstraintRewrite2 constraintRewrite = new ConstraintRewrite2();

        /**********
         if has two "reader" need to write as "reader","reader'"
         ex.
         reader(firstname,lastname,rid,born,gender,phone),reader'(firstname,lastname,rid,born,gender,phone) -: [ false |reader.rid = reader'.rid ,reader.firstname = reader'.firtname]
         reader(a,b,c,d,e,f),                             reader'(g,h,c,i,j,k),....                         -: [ false |a=g,...]
         *********/
        //"reader(a,b,c,d,e,f),reader'(g,h,c,i,j,k) -: [ false |a=g]"
        //constraintRewrite.DCsParse(consText.getText().trim());
        //constraintRewrite.EGDsParse("reader(a,b,c,d,e,f),reader'(g,h,c,i,j,a),... -: a=g, b=h");
        constraintRewrite.parse(constraint);

        /*********
         check the constraint format
         *********/
        if(!constraintRewrite.tbFormatCheck(tableMap)) {
            System.err.println("constraint table schema error");
            return null;
        }

        /*********
         constraint rewrite and get violation tuples
         *********/


        //{ ........ }
        // <"att1 att2 att3",ArrayList<tuples>>
        ArrayList<String> consTbLst = new ArrayList<>();  // all the regarding table in a constraint
        for(TableStru tbStru: constraintRewrite.getTableList()){
            consTbLst.add(tbStru.getTableName());
        }
        //{ ........ }
        String[] depSqlArray = constraintRewrite.rewrite(tableMap);
         // String[ sql , attName1,attName2...]
        HashMap<String,ArrayList<HashMap>> vioTupleMap = constraintRewrite.getVioTuples(depSqlArray,c, tableMap);
        // create deletion table with same structure and store them in the deletion table
        /*
            String createDelTable = "CREATE TABLE del_" +  depSqlArray[0] + sequence + " AS SELECT * FROM " + depSqlArray[0] + " WHERE 1=2;";
            String createDelSql = constraintRewrite.createDeletionTableSql(depSqlArray[0],c, tableMap.get(depSqlArray[0]),vioTuples, sequence);
            postgreSQLJDBC.execute(c,createDelTable,false);
            postgreSQLJDBC.execute(c,createDelSql,false);
            */


        ConstraintStru2 constraintStru = new ConstraintStru2(vioTupleMap,depSqlArray);
        //System.out.println(sql);


        outputText.append(depSqlArray[0] + "\n");


        outputText.repaint();
        outputText.updateUI();
        outputText.validate();

        return constraintStru;
    }

    public void sampleFramework(ArrayList<ConstraintStru2> constraintStruLst){
        int count = 0 ;
        Random random = new Random();
        int m = (int)((1 / (2 * epsilon)) * Math.log(2 / theta));
        System.out.println("m: " + m);

        try {
            //Run Row(SQL(theta)) for each constraint
            for(ConstraintStru2 constraintStru: constraintStruLst){
                if (constraintStru.getDepSqlLst().length > 1){
                    // has equal attribute for different table
                    for(int i=0; i <= m; i++){
                        RandomMarkov randomMarkov = new RandomMarkov(constraintStru,random);
                        // markov chain provide the tuples which to delete next
                        while(randomMarkov.hasNext()){
                            System.out.println(randomMarkov.next());

                        }
                    }
                }



                /******

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
                 ***/
            }

        } catch (Exception e) {
            e.printStackTrace();
            try {
                c.close();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }
        System.out.println(count + "/" + m);
    }
}
