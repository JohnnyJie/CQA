import java.util.ArrayList;
import java.util.HashMap;

public class ConstraintStru2 {
    private ArrayList<HashMap> vioTupleMap;
    private String[] depSqlArray;
    private int sequence;


    public ConstraintStru2(ArrayList<HashMap> vioTupleMap, String[] depSql) {
        this.vioTupleMap = vioTupleMap;
        this.depSqlArray = depSql;
    }

    public  ArrayList<HashMap> getVioTupleMap() {
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
