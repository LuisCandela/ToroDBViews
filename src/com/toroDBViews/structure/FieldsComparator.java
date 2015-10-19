package com.toroDBViews.structure;

import java.io.Serializable;
import java.util.Comparator;

import org.jooq.Field;

public class FieldsComparator implements Comparator<Field>, Serializable {
	

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public int compare(Field field1, Field field2) {

		if (field1.equals(field2)) {
			return 0;
		}
		if (field1.getName().equals("did")) {
			return -1;
		} else {

			return 1;

		}

	}


}
