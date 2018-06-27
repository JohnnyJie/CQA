import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

public class RandomMarkov implements MarkovTree{
    private HashMap<String, ArrayList<HashMap>> vioTupleMap;
    private int index = 0;
    private  Random random;
    private ArrayList<String> keysLst;
    private String[] tableNameLst;

    public RandomMarkov(ConstraintStru2 constraintStru, Random random) {
        this.vioTupleMap = constraintStru.getVioTupleMap();
        this.random = random;
        this.keysLst = new ArrayList(Arrays.asList(vioTupleMap.keySet().toArray(new String[0]))); // the group of violation tuples
        this.tableNameLst = vioTupleMap.keySet().toArray(new String[0]);
    }

    @Override
    public boolean hasNext() {
        return index < keysLst.size(); //
    }

    @Override
    public HashMap next() {
        int size = keysLst.size();
        int pos = Math.abs(random.nextInt()) % size;
        ArrayList<HashMap> tupleLst = vioTupleMap.get(keysLst.get(pos));
        int tupleLstSize = tupleLst.size();
        int pos2 = Math.abs(random.nextInt()) % tupleLstSize;
        HashMap tuple = tupleLst.get(pos2);


        // be careful it seems transfer reference
        vioTupleMap.remove(keysLst.get(pos)); // remove the group of violation tuples in each violation map

        keysLst.remove(pos);
        index ++;

        return tuple;
    }


}
