package com.toroDBViews.structure;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.jooq.Condition;
import org.jooq.CreateViewFinalStep;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Name;
import org.jooq.Param;
import org.jooq.Select;
import org.jooq.impl.DSL;
import org.jooq.types.UInteger;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.toroDBViews.connection.PosgreSQLConnection;
import com.toroDBViews.exceptions.InstanceOfArrayStructureException;
import com.toroDBViews.exceptions.typeAttributeException;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.core.language.AttributeReference;
import com.torodb.torod.core.subdocument.BasicType;
import com.torodb.torod.core.subdocument.SubDocAttribute;
import com.torodb.torod.core.subdocument.SubDocType;
import com.torodb.torod.core.subdocument.structure.DocStructure;
import com.torodb.torod.db.exceptions.InvalidDatabaseException;
import com.torodb.torod.db.postgresql.meta.CollectionSchema;
import com.torodb.torod.db.postgresql.meta.TorodbMeta;
import com.torodb.torod.db.postgresql.meta.tables.SubDocTable;

public class TableCreator {

	private Connection c;
	private PosgreSQLConnection connection = new PosgreSQLConnection();
	private DSLContext dsl;
	private TorodbMeta meta;
	private ViewConsumer viewConsumer;

	public void main(String databaseName) throws SQLException, IOException, InvalidDatabaseException,
			ImplementationDbException, InstanceOfArrayStructureException, InstantiationException,
			IllegalAccessException, ClassNotFoundException, typeAttributeException {

		connection.initialize(databaseName);
		meta = connection.getMeta();
		dsl = connection.getDsl();

		Collection<CollectionSchema> schemes = meta.getCollectionSchemes();

		Table<AttributeReference, Integer, DocStructure> table = HashBasedTable.create();

		for (CollectionSchema colSchema : schemes) {
			table.clear();

			analyzeCollection(colSchema, table);

			dropViews(colSchema, table, databaseName);

			analyzeType(colSchema, table);

			generateView(colSchema, table, databaseName);

		}

	}

	private void analyzeCollection(CollectionSchema colSchema, Table<AttributeReference, Integer, DocStructure> table)
			throws InstanceOfArrayStructureException {

		for (Map.Entry<Integer, DocStructure> entry : colSchema.getStructuresCache().getAllStructures().entrySet()) {

			AnalyzeStructure.analyzeStructure(entry.getKey(), entry.getValue(), table);

		}

	}

	private void dropViews(CollectionSchema colSchema, Table<AttributeReference, Integer, DocStructure> table,
			String databaseName) throws ClassNotFoundException, ImplementationDbException, SQLException {

		c = connection.openConection(databaseName);
		dsl = connection.getDsl();

		for (AttributeReference attRef : table.rowKeySet()) {

			String viewName = setViewName(attRef);
			dsl.dropViewIfExists(DSL.name(colSchema.getName(), viewName)).execute();

			c.commit();

		}
		connection.closeConection();
	}

	private void analyzeType(CollectionSchema colSchema, Table<AttributeReference, Integer, DocStructure> table)
			throws typeAttributeException {

		for (AttributeReference attRef : table.rowKeySet()) {

			for (DocStructure structure1 : table.row(attRef).values()) {
				SubDocType type1 = structure1.getType();

				for (DocStructure structure2 : table.row(attRef).values()) {
					SubDocType type2 = structure2.getType();

					if (type1.equals(type2)) {

					} else {
						Collection<SubDocAttribute> attr1 = type1.getAttributes();
						Collection<SubDocAttribute> attr2 = type2.getAttributes();

						for (SubDocAttribute subDocAttribute1 : attr1) {
							BasicType type3 = subDocAttribute1.getType();

							for (SubDocAttribute subDocAttribute2 : attr2) {
								BasicType type4 = subDocAttribute2.getType();

								if (subDocAttribute1.getKey().equals(subDocAttribute2.getKey())) {

									if (type3.equals(type4) || type3 == BasicType.NULL || type4 == BasicType.NULL) {

									} else {
										throw new typeAttributeException();
									}
								}
							}
						}
					}
				}
			}

		}
	}

	private void generateView(CollectionSchema colSchema, Table<AttributeReference, Integer, DocStructure> table,
			String databaseName) throws ClassNotFoundException, ImplementationDbException, SQLException {

		c = connection.openConection(databaseName);
		dsl = connection.getDsl();

		String schemaName = colSchema.getName();
		org.jooq.Table<?> rootTable = DSL.table(DSL.name(schemaName, "root"));
		Field<Integer> rootSidField = DSL.field(DSL.name(schemaName, "root", "sid"), Integer.class);
		Field<Integer> rootDidField = DSL.field(DSL.name(schemaName, "root", "did"), Integer.class);

		for (AttributeReference attRef : table.rowKeySet()) {
			Select query = null;

			Set<Field<?>> column = addColumnNamesString(attRef, table, colSchema);

			for (Map.Entry<Integer, DocStructure> entry : table.row(attRef).entrySet()) {
				DocStructure docStructure = entry.getValue();
				SubDocType type = docStructure.getType();
				SubDocTable subDocTable = colSchema.getSubDocTable(type);

				Set<Field<?>> fields = Sets.newHashSetWithExpectedSize(column.size());
				fields = addNullFields(column, subDocTable);

				org.jooq.Table<?> joinTable = subDocTable.join(rootTable)
						.on(subDocTable.getDidColumn().eq(rootDidField));

				query = createQuery(query, fields, joinTable, docStructure, subDocTable, rootSidField, entry);

			}

			String viewName = setViewName(attRef);

			Name[] columnNames = setColumName(column);
			
			CreateViewFinalStep view = dsl.createView(DSL.name(schemaName, viewName), columnNames).as(query);
			
			viewConsumer.consume(viewName, view);
			
//			String sql = dsl.createView(DSL.name(schemaName, viewName), columnNames).as(query).toString();
//
//			PreparedStatement statement = c.prepareStatement(sql);
//
//			statement.execute();
//
			c.commit();

		}
		connection.closeConection();
	}

	private Select createQuery(Select query, Set<Field<?>> fields, org.jooq.Table<?> joinTable,
			DocStructure docStructure, SubDocTable subDocTable, Field<Integer> rootSidField,
			Map.Entry<Integer, DocStructure> entry) {

		Condition indexCondition;
		
		if (docStructure.getIndex() == 0) {
			indexCondition = subDocTable.getIndexColumn().isNull();
		} else {
			indexCondition = subDocTable.getIndexColumn().eq(docStructure.getIndex());
		}
		
		Select subQuery = dsl.select(fields).from(joinTable).where(rootSidField.eq(DSL.val(entry.getKey()).cast(Integer.class)).and(indexCondition));
		
		if (query == null) {
			query = subQuery;
		} else {
			query = query.unionAll(subQuery);
		}
		
		return query;
	}

	private String setViewName(AttributeReference attRef) {
		String viewName;

		if (attRef.equals(AttributeReference.EMPTY_REFERENCE)) {
			viewName = "emptyAttRef";
		} else {
			viewName = attRef.toString();
		}

		return viewName;
	}

	private Name[] setColumName(Set<Field<?>> columnNamesString) {

		ColumnComparator comparator = new ColumnComparator();
		Set<String> names = Sets.newTreeSet(comparator);

		for (Field field : columnNamesString) {
			names.add(field.getName());
		}

		Name[] columnNames = new Name[names.size()];

		int i = 0;
		for (String name : names) {

			columnNames[i] = DSL.name(name);

			i++;
		}

		return columnNames;
	}

	private Set<Field<?>> addColumnNamesString(AttributeReference attRef,
			Table<AttributeReference, Integer, DocStructure> table, CollectionSchema colSchema) {

		Set<Field<?>> columnNamesString = Sets.newHashSet();

		for (Map.Entry<Integer, DocStructure> entry : table.row(attRef).entrySet()) {

			SubDocType type = entry.getValue().getType();
			SubDocTable subDocTable = colSchema.getSubDocTable(type);

			List<Field<?>> fields = Lists.newArrayList(subDocTable.fields());
			fields.remove(subDocTable.getIndexColumn());

			for (Field<?> field : fields) {

				columnNamesString.add(field);
			}

		}

		return columnNamesString;
	}

	private Set<Field<?>> addNullFields(Set<Field<?>> columns, SubDocTable subDocTable) {

		FieldsComparator comparator = new FieldsComparator();
		Set<Field<?>> fields = Sets.newTreeSet(comparator);

		for (Field<?> column : columns) {

			if (subDocTable.field(column.getName()) != null) {

				fields.add(subDocTable.field(column));

			} else {

				fields.add(DSL.castNull(column.getDataType()).as(column));
				
			}
		}

		return fields;
	}
}