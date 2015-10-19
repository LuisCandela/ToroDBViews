package com.toroDBViews.structure;

import org.jooq.CreateViewFinalStep;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.toroDBViews.connection.PostgreSQLConnection;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;

public class CreateViewViewConsumer implements ViewConsumer {
	
	private DSLContext dsl;
	
	public CreateViewViewConsumer(PostgreSQLConnection connection) throws ClassNotFoundException, ImplementationDbException {
		connection.openConection();
		dsl = connection.getDsl();
		
	}
	
	@Override
	public void createView(String schemaName, String viewName, CreateViewFinalStep view) {
		view.execute();
				
	}

	@Override
	public void dropView(String schemaName, String viewName) {
		
		dsl.dropViewIfExists(DSL.name(schemaName, viewName)).execute();
	}
	
}
