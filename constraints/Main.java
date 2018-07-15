import javax.swing.*;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
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
    private String constraints = "reader(a,b,c,d,e,f),reader(g,h,c,i,j,k) -: a=g";
    private float epsilon = 0.1f;
    private float theta = 0.01f;
    private static Connection c;
    private PostgreSQLJDBC6 postgreSQLJDBC;
    private HashMap<String,ArrayList> tableMap;
    private ConstraintRewrite2 constraintRewrite;

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
//                    if(main.constraintStruLst != null){
//                        for(ConstraintStru2 constraintStru: main.constraintStruLst){
//                            int sequence = constraintStru.getSequence();
//                            String error_sql = "DROP TABLE delTable"  + sequence + ";";
//                            main.postgreSQLJDBC.execute(c, error_sql,false);
//                        }
//                    }

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
                // invoke the query analysis to get the regarding query tables

                epsilon = Float.valueOf(errorText.getText().trim());  // error bound set by user
                theta = Float.valueOf(confidenceText.getText().trim());  // confidence set by user

                try {
                    int sequence = 0;  // record which constraint
                    for (String constraint : consText.getText().split(";")) {
                        sampleFramework(constraint.trim(),sequence);
                    }
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

        constraintRewrite = new ConstraintRewrite2();

        /**********
         if has two "reader" need to write as "reader","reader'"
         ex.
         reader(firstname,lastname,rid,born,gender,phone),reader'(firstname,lastname,rid,born,gender,phone) -: [ false |reader.rid = reader'.rid ,reader.firstname = reader'.firtname]
         reader(a,b,c,d,e,f),                             reader'(g,h,c,i,j,k),....                         -: [ false |a=g,...]
         *********/
        //"reader(a,b,c,d,e,f),reader'(g,h,c,i,j,k) -: a=g"
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
        ArrayList<HashMap> vioTupleMap = constraintRewrite.getVioTuples(depSqlArray,c, tableMap);

        ConstraintStru2 constraintStru = new ConstraintStru2(vioTupleMap,depSqlArray);


        outputText.append(depSqlArray[0] + "\n");

        outputText.repaint();
        outputText.updateUI();
        outputText.validate();

        return constraintStru;
    }

    public void sampleFramework(String constraint, int sequence) throws SQLException {
        int count = 0 ;
        Random random = new Random();
        int m = (int)((1 / (2 * epsilon)) * Math.log(2 / theta));

        ConstraintStru2 constraintStru= violationCheck(constraint, sequence);

        System.out.println("m: " + m);

        try {
            //Run Row(SQL(theta)) for each constraint

            for(int i=0; i <= m; i++){
                ArrayList<TableStru> tableList = constraintRewrite.getTableList();
                RandomMarkov randomMarkov = new RandomMarkov(constraintStru,random,tableList,tableMap);
                // markov chain provide the tuples which to delete next


                while(randomMarkov.hasNext()){

                    HashMap tuple = randomMarkov.next();
                    // {reader'_rid: xxx,reader'_firstname: xxx ..., tbName:reader'} including table name, need remove "'"

                    System.out.println(tuple);
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(count + "/" + m);
    }
}
