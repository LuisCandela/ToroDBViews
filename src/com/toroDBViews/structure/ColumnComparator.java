package com.toroDBViews.structure;

import java.io.Serializable;
import java.util.Comparator;

public class ColumnComparator implements Comparator<String>, Serializable {
	

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(String field1, String field2) {

		if (field1.equals(field2)) {
			return 0;
		}
		if (field1.equals("did")) {
			return -1;
		} else {

			return 1;

		}

	}


}
