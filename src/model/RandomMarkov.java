package model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class RandomMarkov implements MarkovTree {
	private ArrayList<HashMap> vioTupleLst;
	private Random random;
	private HashMap<String,ArrayList<TableStru>> tableListMap;
	private HashMap<String, ArrayList> tableMap;
	private Boolean[] bitmap;
	private int remainNum;
	private int tupleNumber;

	public RandomMarkov(ViolationStru violationStru, Random random, HashMap<String,ArrayList<TableStru>> tableListMap,
			HashMap<String, ArrayList> tableMap) {
		this.vioTupleLst = (ArrayList<HashMap>) violationStru.getVioTupleMapLst().clone();
		this.random = random;
		this.tableListMap = tableListMap;
		this.tableMap = tableMap;
		this.bitmap = new Boolean[vioTupleLst.size()];
		this.remainNum = vioTupleLst.size();
		this.tupleNumber = vioTupleLst.size();
		for (int i = 0; i < vioTupleLst.size(); i++) {
			bitmap[i] = true;
		}
	}

	@Override
	public boolean hasNext() {
		return remainNum > 0;
	}

	@Override
	public HashMap next() {
		// choose one combined violation tuple, ex reader_rid,reader_firstname
		// ...reader'fistname
		int rand = random.nextInt();
		int bitmapPos = Math.abs(rand) % remainNum;
		int realPos = 0;
		while (bitmapPos >= 0) {
			if (bitmap[realPos]) {
				bitmapPos--;
			}
			realPos++;
		}
		realPos--;
		// find the valid tuple's real position

		HashMap<String, Object> vioTuple = vioTupleLst.get(realPos);
		String sequence = (String)vioTuple.get("sequence");
		// choose a tuple of one table from combined violation tuple, ex.
		// [TB0(x,y,z...),TB1(x1,y1,z1,),...]

		int pos2 = Math.abs(random.nextInt()) % (vioTuple.keySet().size() - 1);

		TableStru tbStru = tableListMap.get(sequence).get(pos2);
		// choose one single part ex.TB0(x,y,z...)
		String tbName = tbStru.getTableName(); // reader
		String nickName = tbStru.getNickName(); // TB0_1 for sequence 1

		ArrayList<String> repeatTbLst = new ArrayList();

		for(String key:tableListMap.keySet()) {
			for (TableStru tableStru :tableListMap.get(key)){
				if (tableStru.getTableName().equals(tbName)) {
					repeatTbLst.add(tableStru.getNickName()); // add the nickname to the repeat table
				}
			}
		}

		HashMap tuple = new HashMap();
		tuple.put("tableName", tbName);
		for (Object attName : tableMap.get(tbName)) {
			tuple.put(attName, ((HashMap)vioTuple.get(nickName)).get(attName));
		}

		// delete all the combined violation tuple contained this tuple
		bitmap[realPos] = false;
		remainNum--;
		for (int i = 0; i < tupleNumber; i++) {
			if (!bitmap[i])
				continue; // false
			for (String repeatTbName : repeatTbLst) {
				HashMap<String, HashMap> remainTuple = vioTupleLst.get(i);
				if (!remainTuple.containsKey(repeatTbName)) {
					continue;
				}
				Boolean bool = true;
				for (Object attName : tableMap.get(tbName)) {
					if ((remainTuple.get(repeatTbName).get(attName) == null && tuple.get(attName) == null)) {
						continue;
					}
					if (remainTuple.get(repeatTbName).get(attName) == null || tuple.get(attName) == null
							|| !remainTuple.get(repeatTbName).get(attName).equals(tuple.get(attName))) {
						bool = false;
						break;
					}
				}
				if (bool) {
					bitmap[i] = false;
					remainNum--;
					break;
				}
			}

		}
		return tuple;
	}

}
