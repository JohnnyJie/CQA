import java.util.ArrayList;

class PKeyDepStru {
    private String constraintName;
    private ArrayList<String> pkeyList;
    private String tableName;

    public PKeyDepStru(String constraintName,String tableName,ArrayList pkeyList){
        this.constraintName = constraintName;
        this.tableName = tableName;
        this.pkeyList = pkeyList;
    }

    public ArrayList<String> getPkeyList() {
        return pkeyList;
    }

    public String getConstraintName() {
        return constraintName;
    }

    public String getTableName() {
        return tableName;
    }
}
