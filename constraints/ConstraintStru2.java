import java.util.ArrayList;
import java.util.HashMap;

public class ConstraintStru2 {
    private HashMap<String,HashMap<String,ArrayList<HashMap>>> vioTupleMap; //HashMap<(String)TableName,(ArrayList)tuples<(HashMap)<attribute name,value>>>
    private ArrayList<String[]> depSqlLst; // [tableName,sql]regarding sql to select all contradictory tuples
    private int sequence;

    public ConstraintStru2(HashMap<String, HashMap<String,ArrayList<HashMap>>> vioTupleMap, ArrayList<String[]> depSql, int sequence) {
        this.vioTupleMap = vioTupleMap;
        this.depSqlLst = depSql;
        this.sequence = sequence;
    }

    public ConstraintStru2(HashMap<String,HashMap<String,ArrayList<HashMap>>> vioTupleMap, ArrayList<String[]> depSql) {
        this.vioTupleMap = vioTupleMap;
        this.depSqlLst = depSql;
    }

    public HashMap<String, HashMap<String,ArrayList<HashMap>>> getVioTupleMap() {
        return vioTupleMap;
    }

    public ArrayList<String[]> getDepSqlLst() {
        return depSqlLst;
    }

    public int getSequence() {
        return sequence;
    }

    public void setSequence(int sequence) {
        this.sequence = sequence;
    }
}
