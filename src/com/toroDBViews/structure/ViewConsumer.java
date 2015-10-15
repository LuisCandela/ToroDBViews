package com.toroDBViews.structure;

import org.jooq.CreateViewFinalStep;

public interface ViewConsumer {
	void consume(String viewName, CreateViewFinalStep view);
}
