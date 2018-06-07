import java.util.ArrayList;

public class ConstraintStru{
    private PKeyDepStru pKeyDepStru;
    private String depSql;

    public ConstraintStru(PKeyDepStru pKeyDepStru, String depSql){
        this.pKeyDepStru = pKeyDepStru;
        this.depSql = depSql;
    }

    public String getDepSql() {
        return depSql;
    }

    public PKeyDepStru getpKeyDepStru() {
        return pKeyDepStru;
    }
}
