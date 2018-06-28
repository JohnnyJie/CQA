import com.sun.org.apache.xpath.internal.operations.Bool;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ConstraintRewrite2 {

    private ArrayList<TableStru> tableList = new ArrayList<>();
    private ArrayList<ConditionStru> condintionLst = new ArrayList();
    private HashMap<String,ArrayList<String>> symbolMap = new HashMap<>();
    private HashMap<String,ArrayList<HashMap>> vioTupleMap = new HashMap<>();


    public void parse(String singleConstraint){
        String rightAtoms = singleConstraint.split("-:")[1].trim();

        String rightAtomsRule = "\\[\\s+false\\s+\\|{1}.*?\\]";
        Pattern rightAtomsPattern = Pattern.compile(rightAtomsRule);
        Matcher rightAtomsMatcher = rightAtomsPattern.matcher(rightAtoms);
        if (rightAtomsMatcher.find()) {
            DCsParse(singleConstraint);
        }else {
            EGDsParse(singleConstraint);
        }

    }

    public void DCsParse(String singleConstraint){ //reader(a,b,c,d,e,f),reader'(g,h,c,i,j,k),... -: [ false |a=g,...]
        try {
            String leftAtoms = singleConstraint.split("-:")[0].trim(); //reader(a,b,c,d,e,f),reader'(g,h,c,i,j,k),...
            String rightAtoms = singleConstraint.split("-:")[1].trim(); //[false | a=g,...]

            /************
             separately extracting the left part (ex. reader(a,b,c,d,e,f),reader'(g,h,c,i,j,k),...)
             ************/

            String leftAtomsRule = ".+?\\({1}.*?\\){1}";
            Pattern leftAtomsPattern = Pattern.compile(leftAtomsRule);
            Matcher leftAtomsMatcher = leftAtomsPattern.matcher(leftAtoms);

            while (leftAtomsMatcher.find()) {
                String tableString = leftAtomsMatcher.group(0).replaceAll(",|\\)"," "); //reader(a,b,c,d,e,f)
                String tableName = tableString.split("\\(")[0].trim(); //reader
                ArrayList attLst = new ArrayList();
                for (String s : tableString.split("\\(")[1].trim().split(" ")){ //a
                    attLst.add(s);  // ArrayList[a , b, ...]
                    if (symbolMap.containsKey(s)){
                        ArrayList<String> newLst = symbolMap.get(s);
                        newLst.add(tableName);
                        symbolMap.put(s,newLst);
                    }else {
                        ArrayList<String> newLst = new ArrayList<String>();
                        newLst.add(tableName);
                        symbolMap.put(s,newLst);
                    }
                }
                TableStru tbStru = new TableStru(tableName,attLst); //TableStru[R1,ArrayList[att1,att2,...]]
                tableList.add(tbStru);
            }
            if (tableList.size()==0)
                throw new Exception("constraint format error");

            /************
             separately extracting the right part (ex. [false | a=g,...])
             ************/

            String rightAtomsRule = "\\[\\s+false\\s+\\|{1}.*?\\]";
            Pattern rightAtomsPattern = Pattern.compile(rightAtomsRule);
            Matcher rightAtomsMatcher = rightAtomsPattern.matcher(rightAtoms);

            if (rightAtomsMatcher.find()) {
                rightAtoms = rightAtomsMatcher.group(0).replaceAll("\\[\\s+false\\s+\\|{1}","");
                rightAtoms = rightAtoms.replaceAll("\\]","").trim();
                // R1.att1 = R2.att1,R2.att1 = R3.att1,...
                for(String s : rightAtoms.split(",")){ // a=g
                    String signRule = "[=|<|<=|>|>=]{1}";
                    String leftTerm = s.split(signRule)[0].trim(); // a
                    String rightTerm = s.split(signRule)[1].trim(); // g
                    String sign = "";
                    Pattern signPattern = Pattern.compile(signRule);
                    Matcher signMatcher = signPattern.matcher(s);
                    if(signMatcher.find()) {
                        sign = signMatcher.group(0); // =
                    }
                    else
                        throw new Exception("constraint format error");

                    condintionLst.add(new ConditionStru(leftTerm,rightTerm,sign,true));
                }
            } else
                throw new Exception("constraint format error");
            if (condintionLst.size()==0)
                throw new Exception("constraint format error");

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void EGDsParse(String singleConstraint){ //reader(a,b,c,d,e,f),reader'(g,h,c,i,j,k),... -: a=g,...
        try {
            String leftAtoms = singleConstraint.split("-:")[0].trim(); //R1(att1,att2,...),R2(att1,att2,...),...
            String rightAtoms = singleConstraint.split("-:")[1].trim(); //R1.att1 = R2.att1,R2.att1 = R3.att1,...


            /************
             separately extracting the left part  (ex. reader(a,b,c,d,e,f),reader'(g,h,c,i,j,k),...)
             ************/

            String leftAtomsRule = ".+?\\({1}.*?\\){1}";
            Pattern leftAtomsPattern = Pattern.compile(leftAtomsRule);
            Matcher leftAtomsMatcher = leftAtomsPattern.matcher(leftAtoms);

            while (leftAtomsMatcher.find()) {
                String tableString = leftAtomsMatcher.group(0).replaceAll(",|\\)"," "); //reader(a,b,...)
                String tableName = tableString.split("\\(")[0].trim(); //reader
                ArrayList attLst = new ArrayList();
                for (String s : tableString.split("\\(")[1].trim().split(" ")){ //att1
                    attLst.add(s);  // ArrayList[a , b, ...]
                    if (symbolMap.containsKey(s)){
                        ArrayList<String> newLst = symbolMap.get(s);
                        newLst.add(tableName);
                        symbolMap.put(s,newLst);
                    }else {
                        ArrayList<String> newLst = new ArrayList<String>();
                        newLst.add(tableName);
                        symbolMap.put(s,newLst);
                    }
                }
                TableStru tbStru = new TableStru(tableName,attLst); //TableStru[R1,ArrayList[att1,att2,...]]
                tableList.add(tbStru);
            }
            if (tableList.size()==0)
                throw new Exception("constraint format error");

            /************
             separately extracting the right part (ex. a=g,...)
             ************/


            // R1.att1 = R2.att1,R2.att1 = R3.att1,...
            for(String s : rightAtoms.split(",")){ // a = g
                String signRule = "[=|<|<=|>|>=]{1}";
                String leftTerm = s.split(signRule)[0].trim(); // a
                String rightTerm = s.split(signRule)[1].trim(); // g
                String sign = "";
                Pattern signPattern = Pattern.compile(signRule);
                Matcher signMatcher = signPattern.matcher(s);
                if(signMatcher.find()) {
                    sign = signMatcher.group(0); // =
                }
                else
                    throw new Exception("constraint format error");

                condintionLst.add(new ConditionStru(leftTerm,rightTerm,sign,false));
            }

            if (condintionLst.size()==0)
                throw new Exception("constraint format error");

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public Boolean tbFormatCheck(HashMap tableMap){
        /**********
         check the constraints' left atoms has the same schema as the table in dbms
         ***********/
        Boolean bool = true;

        for(TableStru tbStru: tableList){
            ArrayList<String> attLst =
                    (ArrayList<String>)tableMap.get(tbStru.getTableName().replaceAll("'",""));
            ArrayList<String> attLst2 = tbStru.getAttList();
            if(attLst != null && attLst.size() == attLst2.size()){

            } else {
                bool = false;
                break;
            }
        }

        return bool;
    }

    /*******
     *
     * @param tableMap
     * @param
     * @return ArrayList<String[queryTbName, sql , attName1,attName2...]>
     * @throws SQLException
     */
    public String[] rewrite( HashMap<String,ArrayList> tableMap) throws SQLException {


        String sql = "SELECT DISTINCT * FROM ";
        String queryTbNickName = "";
        HashMap tbRecord = new HashMap();
        int count = 0;
        for (TableStru tableStru : tableList) {
            String tbName = tableStru.getTableName();
            String nickName = "TB" + count;
            tbRecord.put(nickName, tbName);
            sql += tbName.replaceAll("'", "") + " AS " + nickName + " ,";
            count++;
        }


        //sql = sql.replaceAll("\\*",queryTbNickName + ".*");
        sql = sql.substring(0, sql.length() - 1); //remove the last ","
        sql += " WHERE ";

        //rewrite the equality by finding the same symbol
        ArrayList<String> attNameLst = new ArrayList<>(); //record the attributes name which has equal attribute in different table

        for (Map.Entry entry : symbolMap.entrySet()) {
            // iteratively find regarding tables when a symbol has more than 2 tables using it (c -> reader,reader')
            if (((ArrayList<String>) entry.getValue()).size() >= 2) {

                ArrayList<String> nickNameLst = new ArrayList<>(); //record the nick name of the name
                for (String tbName : ((ArrayList<String>) entry.getValue())) {
                    int index = 0;
                    for (TableStru tableStru : tableList) {
                        if (tableStru.getTableName().equals(tbName)) {
                            index = tableStru.getAttList().indexOf(entry.getKey());  // c -> 2 ("rid" is the 3rd attribute of reader table)
                            break;
                        }
                    }

                    String realTbName = tbName.replaceAll("'", "");
                    // attName records all the regarding attributes in the
                    attNameLst.add(((ArrayList<String>) tableMap.get(realTbName)).get(index));

                    //find the attribute name and add (c -> 2 --(reader)-> rid)
                    for (Object entry2 : tbRecord.entrySet()) {
                        if ((((Map.Entry) entry2).getValue()).equals(tbName)) {
                            nickNameLst.add((String) ((Map.Entry) entry2).getKey());
                        }
                    }
                }

                //start rewrite the equal condition
                for (int i = 0; i < attNameLst.size(); i++) {
                    if (i == 0) continue;
                    sql += nickNameLst.get(i - 1) + "." + attNameLst.get(i - 1) + " = "
                            + nickNameLst.get(i) + "." + attNameLst.get(i) + " AND ";
                }
            }
        }

        // rewrite the condition parts : a=g,...

        for (ConditionStru conditionStru : condintionLst) {
            String leftTerm = conditionStru.getLeftTerm();
            String rightTerm = conditionStru.getRightTerm();

            int leftIndex = 0;
            int rightIndex = 0;

            String leftTbName = (symbolMap.get(leftTerm)).get(0); // only need 1 regarding table
            String rightTbName = (symbolMap.get(rightTerm)).get(0); // only need 1 regarding table

            for (TableStru tableStru : tableList) {
                if (tableStru.getTableName().equals(leftTbName)) {
                    leftIndex = tableStru.getAttList().indexOf(leftTerm);  // c -> 2 ("rid" is the 3rd attribute of reader table)
                    break;
                }
            }
            for (TableStru tableStru : tableList) {
                if (tableStru.getTableName().equals(rightTbName)) {
                    rightIndex = tableStru.getAttList().indexOf(rightTerm);  // c -> 2 ("rid" is the 3rd attribute of reader table)
                    break;
                }
            }

            String leftAttName = ((ArrayList<String>) tableMap.get(leftTbName.replaceAll("'", ""))).get(leftIndex);
            String rightAttName = ((ArrayList<String>) tableMap.get(rightTbName.replaceAll("'", ""))).get(rightIndex);

            String leftTbNickName = "";
            String rightTbNickName = "";

            for (Object entry : tbRecord.entrySet()) {
                if ((((Map.Entry) entry).getValue()).equals(leftTbName)) {
                    leftTbNickName = (String) ((Map.Entry) entry).getKey();
                }
                if ((((Map.Entry) entry).getValue()).equals(rightTbName)) {
                    rightTbNickName = (String) ((Map.Entry) entry).getKey();
                }
            }

            sql += leftTbNickName + "." + leftAttName + " "
                    + conditionStru.getSymbel() + " " + rightTbNickName + "." + rightAttName;
            sql += " AND ";
        }
        sql = sql.substring(0, sql.length() - 4);
        sql += ";";
        String[] depSql = new String[1 + attNameLst.size()];  // [queryTbName, sql , attName]

        depSql[0] = sql;
        for(int i = 0 ; i < attNameLst.size() ; i++){
            depSql[1 + i] =  attNameLst.get(i);
        }


        return depSql;
    }

    public String createDeletionTableSql(String tableName, Connection c, ArrayList<String> attNameLst, ArrayList<HashMap> vioTuples,int sequence) {

        String sql = "INSERT INTO del_" + tableName + sequence + " (";
        for (String attName : attNameLst) {
            sql += attName + ",";
        }
        sql = sql.substring(0,sql.length()-1); //remove the last ","
        sql += ") VALUES ";
        //String sql = "INSERT INTO del_table(a,b) " ;
        for (HashMap tuple: vioTuples) {
            sql += " ( ";
            for (String attName :attNameLst) {
                sql += "'" + tuple.get(attName) + "',";
            }
            sql = sql.substring(0,sql.length()-1); //remove the last ","
            sql += " ),";
        }
        sql = sql.substring(0,sql.length()-1);//remove the last ","
        sql += ";";
        return sql;
    }

    /*********
     *
     * @param depSqlArray String[queryTbName, sql , attName1,attName2...]
     * @param c
     * @param tableMap
     * @return
     * @throws SQLException
     */
    public HashMap<String,ArrayList<HashMap>> getVioTuples(String[] depSqlArray, Connection c,HashMap<String,ArrayList> tableMap) throws SQLException {
        HashMap<String,ArrayList<HashMap>> vioTuples = new HashMap();
        Statement stmt = c.createStatement();
        ResultSet rs = stmt.executeQuery(depSqlArray[0]);


        if(depSqlArray.length > 1){
            // have equal attributes in different tables
            while (rs.next()){
                HashMap tuple = new HashMap<>();

                int index  = 1 ;
                for (TableStru tbStru: tableList){
                    String tbName = tbStru.getTableName();

                    for(Object attName : tableMap.get(tbName.replaceAll("'",""))){
                        String attValue = rs.getString(index); // we can get all type data by getString?
                        index ++;
                        tuple.put(tbName + "_" + attName,attValue);  // reader_rid,reader_firstname ...reader'_rid
                    }
                }


                String key = "";
                for(int i = 0 ; i < depSqlArray.length - 1 ; i++){ // the depSqlArray[0] is sql
                    key += tuple.get(tableList.get(i).getTableName() + "_" + depSqlArray[i + 1]) + ";";
                }
                if(vioTuples.containsKey(key)){
                    ArrayList<HashMap> existLst = vioTuples.get(key);
                    existLst.add(tuple);
                    vioTuples.put(key,existLst);
                }else {
                    ArrayList<HashMap> newExistLst = new ArrayList<>();
                    newExistLst.add(tuple);
                    vioTuples.put(key,newExistLst);
                }
            }
        }else{
            // doesn't have equal attributes in different tables ......

            //.....
            String key = "";
            ArrayList<HashMap> newExistLst = new ArrayList<>();
            vioTuples.put(key,newExistLst);


            while (rs.next()){
                ArrayList<HashMap> existLst = vioTuples.get(key);
                HashMap tuple = new HashMap<>();


                for (TableStru tbStru: tableList){
                    String tbName = tbStru.getTableName();
                    for(Object attName : tableMap.get(tbName)){
                        String attValue = rs.getString((String)attName);
                        tuple.put(tbName + "_" + attName,attValue);
                    }
                }


                existLst.add(tuple);
                vioTuples.put(key,existLst);
            }
            // 还没想好

        }

        stmt.close();
        rs.close();
        return vioTuples;
    }

    public ArrayList<TableStru> getTableList() {
        return tableList;
    }



}

