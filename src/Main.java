import dao.BaseDao;
import model.*;
import utils.ConstraintRewrite2;
import utils.JdbcUtils;

import javax.swing.*;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.sql.Connection;
import java.sql.SQLException;
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
    private JLabel FilterLabel;
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
    private JComboBox queryWay;
    private JLabel isConLabel;
    private JTextField filterField;
    private JButton Filterbutton;

    private String address = "localhost";
    public String dbName = "postgres";
    public String port = "5432";
    public String usrName = "postgres";
    public String psw = "123";
    public String way = "";
    private static String constraints = "borrow(a,b,c),borrow(d,b,e) -:  a=d,c=e";
    private float epsilon = 0.1f;
    private float theta = 0.01f;
    private static Connection c;
    private HashMap<String, ArrayList> tableMap;
    private static JdbcUtils jdbcUtils = new JdbcUtils();
    private BaseDao baseDao = new BaseDao();

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

                    c.close();
                    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
                } catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
        });
        main.initGUI(); // init the components
    }

    public Main() {

    }

    public void initGUI() {
        addrText.setText(address);
        portText.setText(port);
        userText.setText(usrName);
        psdText.setText(psw);
        consText.setText(constraints);
        errorText.setText(String.valueOf(epsilon));
        confidenceText.setText(String.valueOf(theta));
        dbBox.addItem(dbName);


        //init query way
        queryWay.addItem("NOT EXISTS");
        queryWay.addItem("NOT IN");

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
                c = baseDao.connectDB(address, port, dbName, usrName, psw);
                tableMap = baseDao.getTableSchema(c);
                way = queryWay.getSelectedItem().toString().trim();
                isConLabel.setText("Connect to " + dbName);

                dbBox.removeAllItems();
                ArrayList<String> dbLst = baseDao.getDbName(c);
                for (String name : dbLst) {
                    dbBox.addItem(name);
                }
                dbBox.setSelectedItem(dbName);

                outputText.append(tableMap.toString() + "\n");

            }
        });
        // execute query button
        queryBtn.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                super.mouseClicked(e);
                // invoke the query analysis to get the regarding query tables
                if (isConLabel.getText().equals("Not Connected...")) {
                    outputText.append("please connect to database \n");
                    outputText.repaint();
                    outputText.updateUI();
                    outputText.validate();
                } else {
                    epsilon = Float.valueOf(errorText.getText().trim()); // error bound set by user
                    theta = Float.valueOf(confidenceText.getText().trim()); // confidence set by user

                    try {
                        String qText = queryText.getText().trim();
                        System.out.println("query:" + qText);
                        String constraints = consText.getText();
                        sampleFramework(constraints.trim(), qText, way);
                    } catch (Exception e1) {
                        e1.printStackTrace();
                        outputText.append(e1.toString() + "\n");
                        outputText.append("Oops, there is something wrong! \n");
                    }
                }
            }
        });

        Filterbutton.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                super.mouseClicked(e);
                // invoke the query analysis to get the regarding query tables
                float p = Float.parseFloat(filterField.getText());
                String text = outputText.getText();

                outputText.setText("");
                String[] tmp = text.split("\n");
                outputText.append(tmp[0]);

                outputText.append("\n");
                for (String s : tmp) {
                    String[] ptuple = s.split("probablity:");

                    if (ptuple.length > 1) {
                        System.out.println(Float.parseFloat(ptuple[1]));
                        if (Float.parseFloat(ptuple[1]) > p) {
                            outputText.append(s);
                            outputText.append("\n");
                        }
                    }
                }
                outputText.repaint();
                outputText.updateUI();
                outputText.validate();
            }
        });
    }

    /********
     *
     * @param constraint
     * @param
     * @return
     * @throws SQLException
     *             find the violation tuples and regarding table, and save them in
     *             the del_ table
     */
    public ConstraintStru violationCheck(String constraint, String sequence) throws SQLException {
        ConstraintRewrite2 constraintRewrite = new ConstraintRewrite2();

        /**********
         * if has two "reader" need to write as "reader","reader'" ex.
         * reader(firstname,lastname,rid,born,gender,phone),reader'(firstname,lastname,rid,born,gender,phone)
         * -: [ false |reader.rid = reader'.rid ,reader.firstname = reader'.firtname]
         * reader(a,b,c,d,e,f), reader'(g,h,c,i,j,k),.... -: [ false |a=g,...]
         *********/
        constraintRewrite.parse(constraint, sequence);

        /*********
         * check the constraint format
         *********/
        if (!constraintRewrite.tbFormatCheck(tableMap)) {
            System.err.println("constraint table schema error");
            return null;
        }

        /*********
         * constraint rewrite and get violation tuples
         *********/

        // { ........ }
        // <"att1 att2 att3",ArrayList<tuples>>
        ArrayList<String> consTbLst = new ArrayList<>(); // all the regarding table in a constraint
        for (TableStru tbStru : constraintRewrite.getTableList()) {
            consTbLst.add(tbStru.getTableName());
        }
        // { ........ }
        String depSql = constraintRewrite.rewrite(tableMap);
        // String[ sql , attName1,attName2...]
        ArrayList<HashMap> vioTupleMap = constraintRewrite.getVioTuples(depSql, c, tableMap, sequence);

        ConstraintStru constraintStru = new ConstraintStru(vioTupleMap, depSql, constraintRewrite.getTableList(), sequence);
        // System.out.println(sql);

        outputText.repaint();
        outputText.updateUI();
        outputText.validate();

        return constraintStru;
    }

    public void sampleFramework(String constraints, String qText, String way) throws Exception {

        long startTime = System.currentTimeMillis();
        long markovTime = 0;
        long queryRewrite = 0;
        long queryTime = 0;
        BaseDao basedao = new BaseDao();

        Random random = new Random(System.currentTimeMillis());
        int m = (int) ((1 / (2 * epsilon * epsilon)) * Math.log(2 / theta));

        QueriesStru stru = jdbcUtils.splitQuery(qText, c);
        ArrayList<String> tableNames = stru.getTablelist();
        System.out.println(tableNames);
        HashMap<ArrayList<String>, Integer> tupleList = new HashMap<>();
        try {
            // Run Row(SQL(theta)) for each constraint

            for (int i = 0; i < m; i++) {
                ViolationStru violationStru = new ViolationStru();
                int sequence = 0; // record which constraint
                for (String constraint : constraints.split(";")) {
                    ConstraintStru constraintStru = violationCheck(constraint.trim(), String.valueOf(sequence));
                    violationStru.add(constraintStru);
                    sequence++;
                }

                System.out.println("the " + (i + 1) + " round!");

                long eachmarkovTime = 0;
                long eachQueryRewrite = 0;
                long eachQuery = 0;

                HashMap<String, ArrayList<TableStru>> tableStruMap = violationStru.getTableStruMap();

                RandomMarkov randomMarkov = new RandomMarkov(violationStru, random, tableStruMap, tableMap);

                // create delete table for each table
                jdbcUtils.createDeleteTbale(c, tableNames);

                // store tuples to delete
                ArrayList<HashMap> dList = new ArrayList<HashMap>();
                // markov chain provide the tuples which to delete next
                eachmarkovTime = System.currentTimeMillis();
                while (randomMarkov.hasNext()) {

                    HashMap tuple = randomMarkov.next();
                    String tableName = (String) tuple.get("tableName");
                    tuple.put("tableName", "del_" + tableName);
                    dList.add(tuple);
                }
                markovTime += System.currentTimeMillis() - eachmarkovTime;
                System.out.println((i + 1) + "th markov time : " + (System.currentTimeMillis() - eachmarkovTime));

                eachQueryRewrite = System.currentTimeMillis();
                jdbcUtils.InsertData(dList, c);
                if (way.equals("NOT IN")) {
                    jdbcUtils.CreateDeleteView_NOTIN(c, tableNames);
                } else {
                    jdbcUtils.CreateDeleteView_NOTEXIST(c, tableNames);
                }

                queryRewrite += System.currentTimeMillis() - eachQueryRewrite;
                System.out.println((i + 1) + "th deletion table create time : " + (System.currentTimeMillis() - eachQueryRewrite));

                eachQuery = System.currentTimeMillis();

                tupleList = jdbcUtils.queryRewrite(stru, c, tupleList);

                queryTime += System.currentTimeMillis() - eachQuery;
                System.out.println((i + 1) + "th query time  : " + (System.currentTimeMillis() - eachQuery));

                jdbcUtils.DropDView(c, tableNames);
                jdbcUtils.DropDTable(c, tableNames);


            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis(); //获取结束时间
        System.out.println("whole markov time： " + (markovTime) + "ms");
        System.out.println("whole deletion table create time： " + (queryRewrite) + "ms");
        System.out.println("whole query time： " + (queryTime) + "ms");
        System.out.println("running time： " + (endTime - startTime) + "ms");

        for (String att : stru.getAtt()) {
            // System.out.println(att);
            outputText.append(att + "\t");

        }

        outputText.append("probablity\n");

        for (Map.Entry<ArrayList<String>, Integer> entry : tupleList.entrySet()) {
            ArrayList<String> tuple = entry.getKey();

            for (String att : tuple) {

                outputText.append(att + "\t");
            }
            float p = (float) entry.getValue() / (float) m;
            outputText.append("probablity: " + p);
            outputText.append("\n");
        }

        outputText.repaint();
        outputText.updateUI();
        outputText.validate();

    }

}
