import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

public class RandomMarkov implements MarkovTree {
    private ArrayList<HashMap> vioTupleLst;
    private Random random;
    private ArrayList<TableStru> tableList;
    private HashMap<String, ArrayList> tableMap;

    public RandomMarkov(ConstraintStru2 constraintStru, Random random, ArrayList<TableStru> tableList,
                        HashMap<String, ArrayList> tableMap) {
        this.vioTupleLst = (ArrayList<HashMap>) constraintStru.getVioTupleMap().clone();
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
        // choose one combined violation tuple, ex reader_rid,reader_firstname
        // ...reader'fistname
        int size = vioTupleLst.size();
        int pos = Math.abs(random.nextInt()) % size;
        HashMap<String,HashMap> vioTuple = vioTupleLst.get(pos);
        // choose a tuple of one table from combined violation tuple, ex.
        // [TB0(x,y,z...),TB1(x1,y1,z1,),...]
        int pos2 = Math.abs(random.nextInt()) % tableList.size();
        TableStru tbStru = tableList.get(pos2);
        // choose one single part ex.TB0(x,y,z...)
        String tbName = tbStru.getTableName();  // reader
        String nickName = tbStru.getNickName(); // TB0

        ArrayList<String> repeatTbLst = new ArrayList();
        for(TableStru tableStru:tableList){
            if(tableStru.getTableName().equals(tbName)){
                repeatTbLst.add(tableStru.getNickName()); // add the repeat table
            }
        }

        HashMap tuple = new HashMap();
        tuple.put("tableName", tbName);
        for (Object attName : tableMap.get(tbName)) {
            tuple.put(attName, vioTuple.get(nickName).get(attName));
        }
        // delete all the combined violation tuple contained this tuple
        vioTupleLst.remove(pos);
        for(String repeatTbName : repeatTbLst) {
            Iterator<HashMap> iterator = vioTupleLst.iterator();
            while (iterator.hasNext()) {
                HashMap<String,HashMap> remainTuple = iterator.next();
                Boolean bool = true;
                for (Object attName : tableMap.get(tbName.replaceAll("'", ""))) {
                    if ((remainTuple.get(repeatTbName).get(attName) == null && tuple.get(attName) == null)) {
                        continue;
                    }
                    if (remainTuple.get(repeatTbName).get(attName) == null || tuple.get(attName) == null
                            || !remainTuple.get(repeatTbName).get(attName).equals(tuple.get(attName))) {
                        bool = false;
                        break;
                    }
                }
                if (bool)
                    iterator.remove();
            }
        }

        return tuple;
    }

}
