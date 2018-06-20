import java.util.ArrayList;
import java.util.HashMap;

public class ConstraintStru2 {
    private HashMap<String,ArrayList<HashMap>> vioTupleMap;
    private ArrayList<String> depSql;

    public ConstraintStru2(HashMap<String, ArrayList<HashMap>> vioTupleMap, ArrayList<String> depSql) {
        this.vioTupleMap = vioTupleMap;
        this.depSql = depSql;
    }

    public HashMap<String, ArrayList<HashMap>> getVioTupleMap() {
        return vioTupleMap;
    }

    public ArrayList<String> getDepSql() {
        return depSql;
    }
}
