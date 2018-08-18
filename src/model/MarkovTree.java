package model;

import java.util.HashMap;
import java.util.Iterator;

interface MarkovTree extends Iterator {
	ViolationStru violationStru = null;

	public boolean hasNext();

	public HashMap next();
}
