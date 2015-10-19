package com.toroDBViews.structure;

import org.jooq.CreateViewFinalStep;

public interface ViewConsumer {
	void createView(String schema, String viewName, CreateViewFinalStep view);
	void dropView(String schema, String viewName);
}
