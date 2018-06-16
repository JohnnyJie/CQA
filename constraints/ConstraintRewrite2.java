import com.sun.org.apache.xpath.internal.operations.Bool;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ConstraintRewrite2 {
    private ArrayList<TableStru> tableList = new ArrayList<>();
    private ArrayList<ConditionStru> condintionLst = new ArrayList();
    private HashMap<String,ArrayList<String>> symbelMap = new HashMap<>();

    public static void main(String args[]) {
        PostgreSQLJDBC6 postgreSQLJDBC6 = new PostgreSQLJDBC6();
        Connection c = postgreSQLJDBC6.connectDB();
        HashMap<String,ArrayList> tableMap = postgreSQLJDBC6.getTableSchema(c);

        ConstraintRewrite2 constraintRewrite = new ConstraintRewrite2();
        /**********
         if has two "reader" need to write as "reader","reader'"
         ex.
         reader(firstname,lastname,rid,born,gender,phone),reader'(firstname,lastname,rid,born,gender,phone) -: [ false |reader.rid = reader'.rid ,reader.firstname = reader'.firtname]
         reader(a,b,c,d,e,f),                             reader'(g,h,c,i,j,k),....                         -: [ false |a=g,...]
         *********/

        //constraintRewrite.DCsParse("reader(a,b,c,d,e,f),reader'(g,h,c,i,j,a),... -: [ false |a=g, b=h]");
        constraintRewrite.EGDsParse("reader(a,b,c,d,e,f),reader'(g,h,c,i,j,a),... -: a=g, b=h");

        /*********
         check the constraint format
         *********/
        if(!constraintRewrite.tbFormatCheck(constraintRewrite.tableList,tableMap)) {
            System.err.println("constraint table schema error");
            return;
        }

        /*********
         constraint rewrite
         *********/

        String sql = constraintRewrite.rewrite(constraintRewrite.tableList,constraintRewrite.condintionLst,tableMap);
        System.out.println(sql);
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
                    if (symbelMap.containsKey(s)){
                        ArrayList<String> newLst = symbelMap.get(s);
                        newLst.add(tableName);
                        symbelMap.put(s,newLst);
                    }else {
                        ArrayList<String> newLst = new ArrayList<String>();
                        newLst.add(tableName);
                        symbelMap.put(s,newLst);
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
                    if (symbelMap.containsKey(s)){
                        ArrayList<String> newLst = symbelMap.get(s);
                        newLst.add(tableName);
                        symbelMap.put(s,newLst);
                    }else {
                        ArrayList<String> newLst = new ArrayList<String>();
                        newLst.add(tableName);
                        symbelMap.put(s,newLst);
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

    public Boolean tbFormatCheck(ArrayList<TableStru> tableList,HashMap tableMap){
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

    public String rewrite(ArrayList<TableStru> tableList, ArrayList<ConditionStru> conditionLst, HashMap tableMap){
        /**********
         rewrite the constraints into sql
         ***********/
        String sql = "SELECT DISTINCT * FROM ";
        HashMap tbRecord = new HashMap();
        int count = 0;
        for(TableStru tableStru : tableList){
            String tbName = tableStru.getTableName();
            String nickName = "TB" + count;
            tbRecord.put(nickName,tbName);
            sql += tbName.replaceAll("'","") + " AS " + nickName + " ,";
            count ++;
        }
        sql = sql.substring(0,sql.length()-1); //remove the last ","
        sql += " WHERE ";

        //rewrite the equality by finding the same symbol
        Boolean haveEqual = false;
        for(Map.Entry entry : symbelMap.entrySet()){
            // iteratively find regarding tables when a symbol has more than 2 tables using it (c -> reader,reader')
            if(((ArrayList<String>)entry.getValue()).size() >= 2){
                haveEqual = true;
                ArrayList<String> attNameLst = new ArrayList<>(); //record the attribute name
                ArrayList<String> nickNameLst = new ArrayList<>(); //record the nick name of the name
                for(String tbName: ((ArrayList<String>)entry.getValue())){
                    int index = 0;
                    for(TableStru tableStru: tableList){
                        if(tableStru.getTableName().equals(tbName)){
                            index = tableStru.getAttList().indexOf(entry.getKey());  // c -> 2 ("rid" is the 3rd attribute of reader table)
                            break;
                        }
                    }
                    String realTbName = tbName.replaceAll("'","");
                    attNameLst.add(((ArrayList<String>)tableMap.get(realTbName)).get(index));
                    //find the attribute name and add (c -> 2 --(reader)-> rid)
                    for(Object entry2 : tbRecord.entrySet()){
                        if ((((Map.Entry)entry2).getValue()).equals(tbName)){
                            nickNameLst.add((String)((Map.Entry)entry2).getKey());
                        }
                    }
                }
                //start rewrite the equal condition
                for(int i = 0 ; i < attNameLst.size() ; i++){
                    if(i==0) continue;
                    sql += nickNameLst.get(i - 1) + "." + attNameLst.get(i - 1) + " = "
                            + nickNameLst.get(i) + "." + attNameLst.get(i) + " AND ";
                }
            }
        }

        // rewrite the condition parts : a=g,...

        for(ConditionStru conditionStru : conditionLst){
            String leftTerm = conditionStru.getLeftTerm();
            String rightTerm = conditionStru.getRightTerm();

            int leftIndex = 0;
            int rightIndex = 0;

            String leftTbName = (symbelMap.get(leftTerm)).get(0); // only need 1 regarding table
            String rightTbName = (symbelMap.get(rightTerm)).get(0); // only need 1 regarding table

            for(TableStru tableStru: tableList){
                if(tableStru.getTableName().equals(leftTbName)){
                    leftIndex = tableStru.getAttList().indexOf(leftTerm);  // c -> 2 ("rid" is the 3rd attribute of reader table)
                    break;
                }
            }
            for(TableStru tableStru: tableList){
                if(tableStru.getTableName().equals(rightTbName)){
                    rightIndex = tableStru.getAttList().indexOf(rightTerm);  // c -> 2 ("rid" is the 3rd attribute of reader table)
                    break;
                }
            }

            String leftAttName = ((ArrayList<String>)tableMap.get(leftTbName.replaceAll("'",""))).get(leftIndex);
            String rightAttName =((ArrayList<String>)tableMap.get(leftTbName.replaceAll("'",""))).get(rightIndex);

            String leftTbNickName = "";
            String rightTbNickName = "";

            for(Object entry : tbRecord.entrySet()){
                if ((((Map.Entry)entry).getValue()).equals(leftTbName)){
                    leftTbNickName = (String)((Map.Entry)entry).getKey();
                }
                if ((((Map.Entry)entry).getValue()).equals(rightTbName)){
                    rightTbNickName = (String)((Map.Entry)entry).getKey();
                }
            }

            sql += leftTbNickName + "." + leftAttName + " "
                    + conditionStru.getSymbel() + " " + rightTbNickName + "." + rightAttName;
            sql += " AND ";
        }
        sql = sql.substring(0,sql.length()-4);
        sql += ";";
        return sql;
    }

}

