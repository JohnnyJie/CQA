import javax.swing.*;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
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
    private Connection c;
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
        main.initGUI();  // init the components
    }

    public Main() {
        postgreSQLJDBC = new PostgreSQLJDBC6();
        c = postgreSQLJDBC.connectDB(address, port,dbName,usrName,psw);
        tableMap = postgreSQLJDBC.getTableSchema(c);

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

        freshBtn.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                super.mouseClicked(e);
                address = addrText.getText().trim();
                port = portText.getText().trim();
                usrName = userText.getText().trim();
                psw = psdText.getText().trim();
                c = postgreSQLJDBC.connectDB(address, port,dbName,usrName,psw);
            }
        });

        queryBtn.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                super.mouseClicked(e);
                constraintStruLst = new ArrayList<>();

                ArrayList<String> queryTables = new ArrayList<>();
                queryTables.add("reader");
                queryTables.add("reader");

                epsilon = Float.valueOf(errorText.getText().trim());  // error set by user
                theta = Float.valueOf(confidenceText.getText().trim());  // confidence  set by user

                try {
                    for (String constraint : consText.getText().split(";")) {
                            constraintStruLst.add(violationCheck(constraint.trim(),queryTables));  // add violation tuples with regarding sql

                    }
                    sampleFramework(constraintStruLst);
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
        });

    }

    public ConstraintStru2 violationCheck(String constraint,ArrayList<String> queryTbs) throws SQLException {

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
         constraint rewrite
         *********/

        ArrayList<String> sqlLst = constraintRewrite.rewrite(tableMap,queryTbs,c);
        ConstraintStru2 constraintStru = new ConstraintStru2(constraintRewrite.getVioTupleMap(),sqlLst);
        //System.out.println(sql);

        for(String text: sqlLst){
            outputText.append(text + "\n");
        }

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

        //PostgreSQLJDBC5 po = new PostgreSQLJDBC5();
        try {
            HashMap tableMap = postgreSQLJDBC.getTableSchema(c);  //get schema

            //Run Row(SQL(theta)) for each theta
            for(ConstraintStru2 constraintStru: constraintStruLst){

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


            c.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(count + "/" + m);


    }




}
