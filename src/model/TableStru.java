package model;

import java.util.ArrayList;

public class TableStru {
    private String tableName;
    private String nickName;
    private ArrayList attList;

    public TableStru(String tableName,String nickName,ArrayList attList) {
        this.tableName = tableName;
        this.nickName = nickName;
        this.attList = attList;
    }

    public ArrayList getAttList() {
        return attList;
    }

    public String getTableName() {
        return tableName;
    }

    public String getNickName() {
        return nickName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setNickName(String nickName) {
        this.nickName = nickName;
    }

    public void setAttList(ArrayList attList) {
        this.attList = attList;
    }
}
