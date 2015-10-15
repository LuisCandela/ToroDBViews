package com.toroDBViews.mainPruebas;

import com.toroDBViews.structure.TableCreator;

public class Pruebas {

	public static void main(String[] args) {
		
		TableCreator creator = new TableCreator();
		try {
			creator.main("torodb");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
