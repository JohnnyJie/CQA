import java.lang.reflect.Array;
import java.util.*;

public class RandomMarkov implements MarkovTree{
    private ArrayList<HashMap> vioTupleLst;
    private  Random random;
    private ArrayList<TableStru> tableList;
    private HashMap<String,ArrayList> tableMap;

    public RandomMarkov(ConstraintStru2 constraintStru, Random random, ArrayList<TableStru> tableList, HashMap<String,ArrayList> tableMap) {
        this.vioTupleLst = constraintStru.getVioTupleMap();
        this.random = random;
        this.tableList = tableList;
        this.tableMap = tableMap;
    }


    @Override
    public boolean hasNext() {
        return vioTupleLst.size() > 0;
    }

    @Override
    public HashMap next() {
        // choose one combined violation tuple, ex  reader_rid,reader_firstname ...reader'fistname
        int size = vioTupleLst.size();
        int pos = Math.abs(random.nextInt()) % size;
        HashMap vioTuple = vioTupleLst.get(pos);
        // choose a tuple of one table from combined violation tuple, ex. reader_rid,reader_firstname ...reader_phone
        int pos2 =  Math.abs(random.nextInt()) % tableList.size();
        TableStru tbStru = tableList.get(pos2);
        String tbName = tbStru.getTableName();
        HashMap tuple = new HashMap();
        for(Object attName : tableMap.get(tbName.replaceAll("'",""))){
            tuple.put(attName,vioTuple.get(tbName + "_" + attName));
        }
        // delete all the combined violation tuple contained this tuple
        vioTupleLst.remove(pos);
        Iterator<HashMap> iterator = vioTupleLst.iterator();
        while(iterator.hasNext()){
            HashMap remainTuple = iterator.next();
            Boolean bool = true;
            for(Object attName : tableMap.get(tbName.replaceAll("'",""))){

                if((remainTuple.get(tbName + "_" + attName) == null && tuple.get(attName) == null) ){
                    continue;
                }
                if( remainTuple.get(tbName + "_" + attName) == null || tuple.get(attName) == null || !remainTuple.get(tbName + "_" + attName).equals(tuple.get(attName)) ){
                    bool = false;
                    break;
                }
            }
            if (bool) iterator.remove();
        }

        return tuple;
    }


}
