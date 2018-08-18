package model;

import java.util.ArrayList;
import java.util.HashMap;


public class ViolationStru {
    private ArrayList<HashMap> vioTupleMapLst;
    private ArrayList<String> depSql;
    private HashMap<String,ArrayList<TableStru>> tableStruMap;


    public ViolationStru(){
        this.vioTupleMapLst = new ArrayList<>();
        this.depSql = new ArrayList<>();
        this.tableStruMap = new HashMap<>();
    }

    public void add(ConstraintStru constraintStru) {
        this.vioTupleMapLst.addAll(constraintStru.getVioTupleMap());
        this.depSql.add(constraintStru.getDepSql());
        this.tableStruMap.put(constraintStru.getSequence(),constraintStru.getTableList());
    }

    public ArrayList<HashMap> getVioTupleMapLst() {
        return vioTupleMapLst;
    }

    public ArrayList<String> getDepSql() {
        return depSql;
    }


    public HashMap<String, ArrayList<TableStru>> getTableStruMap() {
        return tableStruMap;
    }
}