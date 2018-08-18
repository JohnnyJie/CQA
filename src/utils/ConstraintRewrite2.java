package utils;

import com.sun.org.apache.xpath.internal.operations.Bool;
import model.ConditionStru;
import model.TableStru;

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


    public void parse(String singleConstraint, String sequence){
        // EGD: reader(a,b,c,d,e,f),reader'(g,h,c,i,j,k) -: a=g
        // DC: reader(a,b,c,d,e,f),reader'(g,h,c,i,j,k),a = g -: false

        String rightAtoms = singleConstraint.split("-:")[1].trim();
        String rightAtomsRule = ".*?false";

        Pattern rightAtomsPattern = Pattern.compile(rightAtomsRule);
        Matcher rightAtomsMatcher = rightAtomsPattern.matcher(rightAtoms);

        if (rightAtomsMatcher.matches()) {
            DCsParse(singleConstraint, sequence);
        }else {
            EGDsParse(singleConstraint, sequence);
        }

    }

    public void DCsParse(String singleConstraint, String sequence){ //reader(a,b,c,d,e,f),reader(g,h,c,i,j,k),a=g,... -:  false
        try {
            /************
             check the format
             ************/

            String dcFormatRule = "(.+?\\(.*?\\))(,|.+?\\(.*?\\))*(,|.+?[<=>]+?.+?)*\\s*-:\\s*false\\s*";
            Pattern dcFormatPattern = Pattern.compile(dcFormatRule);
            Matcher dcFormatMatcher = dcFormatPattern.matcher(singleConstraint);
            if (!dcFormatMatcher.matches()) throw new Exception("constraint format error");

            String leftAtoms = singleConstraint.split("-:")[0].trim(); // reader(a,b,c,d,e,f),reader(g,h,c,i,j,k),a=g,...
            String rightAtoms = singleConstraint.split("-:")[1].trim(); // false


            /************
             separately extracting the table in the left part (ex. reader(a,b,c,d,e,f),reader(g,h,c,i,j,k),...)
             ************/

            String tableRule = ".+?\\({1}.*?\\){1}";
            Pattern tablePattern = Pattern.compile(tableRule);
            Matcher tableMatcher = tablePattern.matcher(leftAtoms);

            int tableCount = 0;
            while (tableMatcher.find()) {
                String tableString = tableMatcher.group(0).replaceAll(",|\\)"," "); //reader(a,b,c,d,e,f)
                String tableName = tableString.split("\\(")[0].trim(); //reader
                String nickName = "TB" + tableCount + "_" + sequence; // TB0_1 for sequence 1
                ArrayList attLst = new ArrayList();
                for (String s : tableString.split("\\(")[1].trim().split(" ")){ //a
                    attLst.add(s);  // ArrayList[a , b, ...]
                    if (symbolMap.containsKey(s)){ // record the usage of the symbol in the table
                        ArrayList<String> newLst = symbolMap.get(s);
                        newLst.add(nickName);
                        symbolMap.put(s,newLst);
                    }else {
                        ArrayList<String> newLst = new ArrayList<String>();
                        newLst.add(nickName);
                        symbolMap.put(s,newLst);
                    }
                }
                TableStru tbStru = new TableStru(tableName,nickName,attLst); //TableStru[R1,ArrayList[att1,att2,...]]
                tableList.add(tbStru);
                tableCount ++;
            }
            if (tableList.size()==0)
                throw new Exception("constraint format error");

            /************
             separately extracting the condition in the left part (ex.  a=g,...)
             ************/
            String conditionRule = "(\\s*[0-9a-zA-Z.?!]+?\\s*[<=>]+?\\s*[0-9a-zA-Z.?!]+?\\s*)";
            Pattern conditionPattern = Pattern.compile(conditionRule);
            Matcher conditionMatcher = conditionPattern.matcher(leftAtoms);

            // R1.att1 = R2.att1,R2.att1 = R3.att1,...

            while (conditionMatcher.find()) {
                // a=g
                String s = conditionMatcher.group(0).replaceAll(",|\\)", " ");
                String signRule = "(=|<>|>=|<=|<|>){1}";
                String leftTerm = s.split(signRule)[0].trim(); // a
                String rightTerm = s.split(signRule)[1].trim(); // g
                String sign = "";
                Pattern signPattern = Pattern.compile(signRule);
                Matcher signMatcher = signPattern.matcher(s);
                if (signMatcher.find() && rightTerm != null && leftTerm != null) {
                    sign = signMatcher.group(0); // =
                } else
                    throw new Exception("constraint format error");

                condintionLst.add(new ConditionStru(leftTerm, rightTerm, sign, false));
            }


        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void EGDsParse(String singleConstraint, String sequence){ //reader(a,b,c,d,e,f),reader(g,h,c,i,j,k),... -: a=g,...
        try {

            /************
             check the format
             ************/

            String egdFormatRule = "(.+?\\(.*?\\))(,|.+?\\(.*?\\))*\\s*-:\\s*(,|.+?[<=>]+?.+?)*\\s*";
            Pattern egdFormatPattern = Pattern.compile(egdFormatRule);
            Matcher egdFormatMatcher = egdFormatPattern.matcher(singleConstraint);
            if (!egdFormatMatcher.matches()) throw new Exception("constraint format error");

            String leftAtoms = singleConstraint.split("-:")[0].trim(); //R1(att1,att2,...),R2(att1,att2,...),...
            String rightAtoms = singleConstraint.split("-:")[1].trim(); //R1.att1 = R2.att1,R2.att1 = R3.att1,...


            /************
             separately extracting the left part  (ex. reader(a,b,c,d,e,f),reader'(g,h,c,i,j,k),...)
             ************/

            String leftAtomsRule = ".+?\\({1}.*?\\){1}";
            Pattern leftAtomsPattern = Pattern.compile(leftAtomsRule);
            Matcher leftAtomsMatcher = leftAtomsPattern.matcher(leftAtoms);

            int tableCount = 0;
            while (leftAtomsMatcher.find()) {
                String tableString = leftAtomsMatcher.group(0).replaceAll(",|\\)"," "); //reader(a,b,...)
                String tableName = tableString.split("\\(")[0].trim(); //reader
                String nickName = "TB" + tableCount + "_" + sequence; // TB0_1 for sequence 1
                ArrayList attLst = new ArrayList();
                for (String s : tableString.split("\\(")[1].trim().split(" ")){ //att1
                    attLst.add(s);  // ArrayList[a , b, ...]
                    if (symbolMap.containsKey(s)){  // record the usage of the symbol in the table
                        ArrayList<String> newLst = symbolMap.get(s);
                        newLst.add(nickName);
                        symbolMap.put(s,newLst);
                    }else {
                        ArrayList<String> newLst = new ArrayList<String>();
                        newLst.add(nickName);
                        symbolMap.put(s,newLst);
                    }
                }

                TableStru tbStru = new TableStru(tableName,nickName,attLst); //TableStru[reader,TB0,ArrayList[att1,att2,...]]
                tableList.add(tbStru);
                tableCount ++;
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

                condintionLst.add(new ConditionStru(leftTerm,rightTerm,sign,true));
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
    public String rewrite( HashMap<String,ArrayList> tableMap) throws SQLException {

        /**********
         rewrite the first part of SQL, ex("SELECT DISTINCT * FROM reader as TB0, reader as TB1 WHERE)
         ***********/
        String sql = "SELECT DISTINCT * FROM ";
        HashMap tbRecord = new HashMap();
        for (TableStru tableStru : tableList) {
            String tbName = tableStru.getTableName(); // reader
            String nickName = tableStru.getNickName(); // TB0_1
            tbRecord.put(nickName, tbName);
            sql += tbName.replaceAll("'", "") + " AS " + nickName + " ,";
        }


        sql = sql.substring(0, sql.length() - 1); //remove the last ","
        sql += " WHERE ";

        /**********
         rewrite the equality by finding the same symbol in different table
         find the attribute name by the regarding symbol
         ***********/

        for (Map.Entry entry : symbolMap.entrySet()) {
            ArrayList<String> attNameLst = new ArrayList<>(); //record the attributes name which has equal attribute in different table
            // iteratively find regarding tables when a symbol has more than 2 tables using it (c -> TB0,TB1)
            if (((ArrayList<String>) entry.getValue()).size() >= 2) {
                ArrayList<String> usedNickNameList = new ArrayList<>(); // store the used nick name

                ArrayList<String> nickNameLst = new ArrayList<>(); //record the nickname of the name


                for (String nickName : ((ArrayList<String>) entry.getValue())) {
                    if(usedNickNameList.contains(nickName)) continue;
                    String realTbName = "";
                    ArrayList<Integer> indexList = new ArrayList();
                    for (TableStru tableStru : tableList) {
                        if (tableStru.getNickName().equals(nickName)) {  // find real table name by nickname
                            usedNickNameList.add(nickName); // add the nick name

                            for(int i = 0; i<tableStru.getAttList().size(); i++){
                                if(tableStru.getAttList().get(i).equals(entry.getKey())){
                                    indexList.add(i);
                                }
                            }
                            // c -> 2 ("rid" is the 3rd attribute of reader table)
                            realTbName = tableStru.getTableName();
                            break;
                        }
                    }

                    // find the attribute name by symbol attName records all the regarding attributes in the
                    for(int index: indexList){
                        attNameLst.add(((ArrayList<String>) tableMap.get(realTbName)).get(index));
                        nickNameLst.add(nickName);
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
        /**********
         rewrite the condition parts : a=g,...
         ***********/

        for (ConditionStru conditionStru : condintionLst) {
            String leftTerm = conditionStru.getLeftTerm();  //a
            String rightTerm = conditionStru.getRightTerm();  //g

            Pattern numericPattern = Pattern.compile("^[-\\+]?[\\d|.]*?$");
            // find the left part of condition, numeric or attribute
            String leftCondition = "";
            if(numericPattern.matcher(leftTerm).matches()){
                leftCondition = leftTerm;
            }else {
                String leftTbName = "";
                int leftIndex = 0;
                String leftTbNickName = (symbolMap.get(leftTerm)).get(0); // only need 1 regarding table, ex. TB0
                for (TableStru tableStru : tableList) {
                    if (tableStru.getNickName().equals(leftTbNickName)) {
                        leftIndex = tableStru.getAttList().indexOf(leftTerm);  // c -> 2 ("rid" is the 3rd attribute of reader table)
                        leftTbName = tableStru.getTableName();
                        break;
                    }
                }
                String leftAttName = ((ArrayList<String>) tableMap.get(leftTbName.replaceAll("'", ""))).get(leftIndex);
                leftCondition = leftTbNickName + "." + leftAttName;
            }
            // find the right part of condition, numeric or attribute
            String rightCondition = "";
            if(numericPattern.matcher(rightTerm).matches()){
                rightCondition = rightTerm;
            }else {
                int rightIndex = 0;
                String rightTbNickName = (symbolMap.get(rightTerm)).get(0); // only need 1 regarding table
                String rightTbName = "";
                for (TableStru tableStru : tableList) {
                    if (tableStru.getNickName().equals(rightTbNickName)) {
                        rightIndex = tableStru.getAttList().indexOf(rightTerm);  // c -> 2 ("rid" is the 3rd attribute of reader table)
                        rightTbName = tableStru.getTableName();
                        break;
                    }
                }
                String rightAttName = ((ArrayList<String>) tableMap.get(rightTbName.replaceAll("'", ""))).get(rightIndex);
                rightCondition = rightTbNickName + "." + rightAttName;
            }


            sql += leftCondition + " " + conditionStru.getSymbel() + " " + rightCondition;
            sql += " AND ";
        }
        sql = sql.substring(0, sql.length() - 4);
        sql += ";";

        return sql;
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
     * @param sql String[queryTbName, sql , attName1,attName2...]
     * @param c
     * @param tableMap
     * @return
     * @throws SQLException
     */
    public ArrayList<HashMap> getVioTuples(String sql, Connection c,HashMap<String,ArrayList> tableMap, String sequence) throws SQLException {
        ArrayList<HashMap> vioTuples = new ArrayList<>();  // <tableName,<attributeName,value>>
        Statement stmt = c.createStatement();
        ResultSet rs = stmt.executeQuery(sql);


        // have equal attributes in different tables
        while (rs.next()){

            HashMap completeTuple = new HashMap<>();
			// record the complete query answer and separate by nickname [TB0(x,y,z...),TB1(x1,y1,z1,),...]
            int index  = 1 ;
            for (TableStru tbStru: tableList){
                String tbName = tbStru.getTableName();
				String nickName = tbStru.getNickName();
				HashMap partTuple = new HashMap<>();
				// TB0(x,y,z...)
                for(Object attName : tableMap.get(tbName)){
                    String attValue = rs.getString(index); // we can get all type data by getString?
                    index ++;
					partTuple.put(attName,attValue);  // reader_rid,reader_firstname ...reader'_rid
                }
				completeTuple.put(nickName,partTuple);
                completeTuple.put("sequence",sequence);
            }
            vioTuples.add(completeTuple);
        }



        stmt.close();
        rs.close();
        return vioTuples;
    }

    public ArrayList<TableStru> getTableList() {
        return tableList;
    }



}

