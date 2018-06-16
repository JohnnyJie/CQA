import java.util.ArrayList;

public class TableStru {
    private String tableName;
    private ArrayList attList;

    public TableStru(String tableName,ArrayList attList) {
        this.tableName = tableName;
        this.attList = attList;
    }

    public ArrayList getAttList() {
        return attList;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setAttList(ArrayList attList) {
        this.attList = attList;
    }
}
