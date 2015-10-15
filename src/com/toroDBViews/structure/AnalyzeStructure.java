package com.toroDBViews.structure;

import java.util.Map.Entry;

import com.google.common.collect.Table;
import com.toroDBViews.exceptions.InstanceOfArrayStructureException;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.language.AttributeReference.ObjectKey;
import com.torodb.torod.core.subdocument.structure.ArrayStructure;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.core.subdocument.structure.StructureElement;


public class AnalyzeStructure {

	public static void analyzeStructure(int sid, DocStructure node, Table<AttributeReference,Integer, DocStructure> table) throws InstanceOfArrayStructureException {
		analyzeStructure(sid, node, table, AttributeReference.EMPTY_REFERENCE);
	}
	
	private static void analyzeStructure(int sid, DocStructure node, Table< AttributeReference,Integer, DocStructure> table,
			AttributeReference path) throws InstanceOfArrayStructureException {
		
		table.put(path,sid, node);
		
		
		
		
		for(Entry<String, StructureElement> entry : node.getElements().entrySet() ){
			
			if(entry.getValue() instanceof ArrayStructure){
				
				throw new InstanceOfArrayStructureException();
				
			}else{
				
				AttributeReference path2 = path.append(new ObjectKey(entry.getKey()));
				
				analyzeStructure(sid, (DocStructure) entry.getValue(), table, path2);
				
			}
		}
	}
}
