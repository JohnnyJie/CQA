import java.util.ArrayList;
import java.util.HashMap;

public class ConstraintStru2 {
    private HashMap<String,ArrayList<HashMap>> vioTupleMap; //HashMap<(String)TableName,(ArrayList)tuples<(HashMap)<attribute name,value>>>
    private String[] depSqlArray; // [tableName,sql]regarding sql to select all contradictory tuples
    private int sequence;

    public ConstraintStru2( HashMap<String,ArrayList<HashMap>> vioTupleMap, String[] depSqlArray, int sequence) {
        this.vioTupleMap = vioTupleMap;
        this.depSqlArray = depSqlArray;
        this.sequence = sequence;
    }

    public ConstraintStru2(HashMap<String,ArrayList<HashMap>> vioTupleMap, String[] depSql) {
        this.vioTupleMap = vioTupleMap;
        this.depSqlArray = depSql;
    }

    public  HashMap<String,ArrayList<HashMap>> getVioTupleMap() {
        return vioTupleMap;
    }

    public String[] getDepSqlLst() {
        return depSqlArray;
    }

    public int getSequence() {
        return sequence;
    }

    public void setSequence(int sequence) {
        this.sequence = sequence;
    }
}
