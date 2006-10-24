/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
 * 
 * This library is free software; you can redistribute it and/or modify it 
 * under the terms of the GNU Lesser General Public License as published by the 
 * Free Software Foundation; either version 2.1 of the License, or (at your 
 * option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
 * for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, 
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 * 
 * Contact: ferraro@users.sourceforge.net
 */
package net.sf.hajdbc.dialect;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.UniqueConstraint;
import net.sf.hajdbc.cache.ForeignKeyConstraintImpl;
import net.sf.hajdbc.cache.UniqueConstraintImpl;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@Test
public class TestDefaultDialect implements Dialect
{
	protected IMocksControl control = EasyMock.createStrictControl();
	protected TableProperties tableProperties = this.control.createMock(TableProperties.class);
	protected Connection connection = this.control.createMock(Connection.class);
	protected ColumnProperties columnProperties = this.control.createMock(ColumnProperties.class);
	protected DatabaseMetaData metaData = this.control.createMock(DatabaseMetaData.class);
	protected Statement statement = this.control.createMock(Statement.class);
	protected ResultSet resultSet = this.control.createMock(ResultSet.class);
	
	protected Dialect dialect = this.createDialect();
	
	protected Dialect createDialect()
	{
		return new DefaultDialect();
	}
	
	@AfterMethod
	void reset()
	{
		this.control.reset();
	}
	
	@DataProvider(name = "table")
	Object[][] tableProvider()
	{
		return new Object[][] { new Object[] { this.tableProperties } };
	}
	
	@DataProvider(name = "foreign-key")
	Object[][] foreignKeyProvider()
	{
		ForeignKeyConstraint foreignKey = new ForeignKeyConstraintImpl("name", "table");
		foreignKey.getColumnList().add("column1");
		foreignKey.getColumnList().add("column2");
		foreignKey.setForeignTable("foreign_table");
		foreignKey.getForeignColumnList().add("foreign_column1");
		foreignKey.getForeignColumnList().add("foreign_column2");
		foreignKey.setDeferrability(DatabaseMetaData.importedKeyInitiallyDeferred);
		foreignKey.setDeleteRule(DatabaseMetaData.importedKeyCascade);
		foreignKey.setUpdateRule(DatabaseMetaData.importedKeyRestrict);
		
		return new Object[][] { new Object[] { foreignKey } };
	}
	
	@DataProvider(name = "unique-constraint")
	Object[][] uniqueConstraintProvider()
	{
		UniqueConstraint uniqueKey = new UniqueConstraintImpl("name", "table");
		uniqueKey.getColumnList().add("column1");
		uniqueKey.getColumnList().add("column2");
		
		return new Object[][] { new Object[] { uniqueKey } };
	}
	
	@DataProvider(name = "alter-sequence")
	Object[][] alterSequenceProvider()
	{
		return new Object[][] { new Object[] { "sequence", 1L } };
	}
	
	@DataProvider(name = "column")
	Object[][] columnProvider()
	{
		return new Object[][] { new Object[] { this.columnProperties } };
	}

	@DataProvider(name = "connection")
	Object[][] connectionProvider()
	{
		return new Object[][] { new Object[] { this.connection } };
	}

	@DataProvider(name = "null")
	Object[][] nullProvider()
	{
		return new Object[][] { new Object[] { null } };
	}

	@DataProvider(name = "sequence")
	Object[][] sequenceProvider()
	{
		return new Object[][] { new Object[] { "sequence" } };
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getAlterSequenceSQL(java.lang.String, long)
	 */
	@Test(dataProvider = "alter-sequence")
	public String getAlterSequenceSQL(String sequence, long value) throws SQLException
	{
		this.control.replay();
		
		String sql = this.dialect.getAlterSequenceSQL(sequence, value);
		
		this.control.verify();
		
		assert sql.equals("ALTER SEQUENCE sequence RESTART WITH 1") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getColumnType(net.sf.hajdbc.ColumnProperties)
	 */
	@Test(dataProvider = "column")
	public int getColumnType(ColumnProperties properties) throws SQLException
	{
		EasyMock.expect(properties.getType()).andReturn(Types.INTEGER);
		
		this.control.replay();
		
		int type = this.dialect.getColumnType(properties);
		
		this.control.verify();
		
		assert type == Types.INTEGER : type;
		
		return type;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getCreateForeignKeyConstraintSQL(net.sf.hajdbc.ForeignKeyConstraint)
	 */
	@Test(dataProvider = "foreign-key")
	public String getCreateForeignKeyConstraintSQL(ForeignKeyConstraint constraint) throws SQLException
	{
		this.control.replay();
		
		String sql = this.dialect.getCreateForeignKeyConstraintSQL(constraint);
		
		assert sql.equals("ALTER TABLE table ADD CONSTRAINT name FOREIGN KEY (column1, column2) REFERENCES foreign_table (foreign_column1, foreign_column2) ON DELETE CASCADE ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getCreateUniqueConstraintSQL(net.sf.hajdbc.UniqueConstraint)
	 */
	@Test(dataProvider = "unique-constraint")
	public String getCreateUniqueConstraintSQL(UniqueConstraint constraint) throws SQLException
	{
		this.control.replay();
		
		String sql = this.dialect.getCreateUniqueConstraintSQL(constraint);
		
		assert sql.equals("ALTER TABLE table ADD CONSTRAINT name UNIQUE (column1, column2)") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getDropForeignKeyConstraintSQL(net.sf.hajdbc.ForeignKeyConstraint)
	 */
	@Test(dataProvider = "foreign-key")
	public String getDropForeignKeyConstraintSQL(ForeignKeyConstraint constraint) throws SQLException
	{
		this.control.replay();
		
		String sql = this.dialect.getDropForeignKeyConstraintSQL(constraint);
		
		assert sql.equals("ALTER TABLE table DROP CONSTRAINT name") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getDropUniqueConstraintSQL(net.sf.hajdbc.UniqueConstraint)
	 */
	@Test(dataProvider = "unique-constraint")
	public String getDropUniqueConstraintSQL(UniqueConstraint constraint) throws SQLException
	{
		this.control.replay();
		
		String sql = this.dialect.getDropUniqueConstraintSQL(constraint);
		
		assert sql.equals("ALTER TABLE table DROP CONSTRAINT name") : sql;
		
		return sql;
	}
	
	/**
	 * @throws SQLException 
	 * @see net.sf.hajdbc.Dialect#getLockTableSQL(net.sf.hajdbc.TableProperties)
	 */
	@Test(dataProvider = "table")
	public String getLockTableSQL(TableProperties properties) throws SQLException
	{
		UniqueConstraint primaryKey = new UniqueConstraintImpl("name", "table");
		primaryKey.getColumnList().add("column1");
		primaryKey.getColumnList().add("column2");
		
		EasyMock.expect(properties.getName()).andReturn("table");
		EasyMock.expect(properties.getPrimaryKey()).andReturn(primaryKey);
		
		this.control.replay();
		
		String sql = this.dialect.getLockTableSQL(properties);
		
		this.control.verify();
		
		assert sql.equals("UPDATE table SET column1 = column1, column2 = column2") : sql;
		
		this.control.reset();
		
		EasyMock.expect(properties.getName()).andReturn("table");
		EasyMock.expect(properties.getPrimaryKey()).andReturn(null);
		EasyMock.expect(properties.getColumns()).andReturn(primaryKey.getColumnList());
		
		this.control.replay();
		
		sql = this.dialect.getLockTableSQL(properties);
		
		this.control.verify();
		
		assert sql.equals("UPDATE table SET column1 = column1, column2 = column2") : sql;
		
		return sql;
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getCurrentSequenceValueSQL(java.lang.String)
	 */
	@Test(dataProvider = "sequence")
	public String getCurrentSequenceValueSQL(String sequence) throws SQLException
	{
		this.control.replay();
		
		String sql = this.dialect.getCurrentSequenceValueSQL(sequence);
		
		this.control.verify();
		
		assert sql.equals("SELECT CURRENT VALUE FOR sequence") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getSequences(java.sql.Connection)
	 */
	@Test(dataProvider = "connection")
	public Collection<String> getSequences(Connection connection) throws SQLException
	{
		EasyMock.expect(connection.getMetaData()).andReturn(this.metaData);
		EasyMock.expect(this.metaData.getTables(EasyMock.eq(""), EasyMock.eq((String) null), EasyMock.eq("%"), EasyMock.aryEq(new String[] { "SEQUENCE" }))).andReturn(this.resultSet);
		EasyMock.expect(this.resultSet.next()).andReturn(true);
		EasyMock.expect(this.resultSet.getString("TABLE_SCHEM")).andReturn("schema");
		EasyMock.expect(this.resultSet.getString("TABLE_NAME")).andReturn("sequence1");
		EasyMock.expect(this.resultSet.next()).andReturn(true);
		EasyMock.expect(this.resultSet.getString("TABLE_SCHEM")).andReturn("schema");
		EasyMock.expect(this.resultSet.getString("TABLE_NAME")).andReturn("sequence2");
		EasyMock.expect(this.resultSet.next()).andReturn(false);
		
		this.resultSet.close();
		
		this.control.replay();
		
		Collection<String> sequences = this.dialect.getSequences(connection);
		
		this.control.verify();
		
		assert sequences.size() == 2 : sequences;
		
		Iterator<String> iterator = sequences.iterator();
		String sequence = iterator.next();
		
		assert sequence.equals("schema.sequence1") : sequence;
		
		sequence = iterator.next();
		
		assert sequence.equals("schema.sequence2") : sequence;
		
		return sequences;
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getSimpleSQL()
	 */
	@Test
	public String getSimpleSQL() throws SQLException
	{
		this.control.replay();
		
		String sql = this.dialect.getSimpleSQL();
		
		this.control.verify();
		
		assert sql.equals("SELECT CURRENT_TIMESTAMP") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getTruncateTableSQL(net.sf.hajdbc.TableProperties)
	 */
	@Test(dataProvider = "table")
	public String getTruncateTableSQL(TableProperties properties) throws SQLException
	{
		EasyMock.expect(properties.getName()).andReturn("table");
		
		this.control.replay();
		
		String sql = this.dialect.getTruncateTableSQL(properties);
		
		this.control.verify();
		
		assert sql.equals("DELETE FROM table");
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#isSelectForUpdate(java.lang.String)
	 */
	@Test(dataProvider = "null")
	public boolean isSelectForUpdate(String sql) throws SQLException
	{
		this.control.replay();
		
		boolean selectForUpdate = this.dialect.isSelectForUpdate("SELECT * FROM table FOR UPDATE");
		
		this.control.verify();
		
		assert selectForUpdate;
		
		this.control.reset();
		this.control.replay();
		
		selectForUpdate = this.dialect.isSelectForUpdate("SELECT * FROM table");
		
		this.control.verify();
		
		assert !selectForUpdate;
		
		return selectForUpdate;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#parseSequence(java.lang.String)
	 */
	@Test(dataProvider = "null")
	public String parseSequence(String sql) throws SQLException
	{
		this.control.replay();
		
		String sequence = this.dialect.parseSequence("SELECT NEXT VALUE FOR sequence");
		
		this.control.verify();
		
		assert sequence.equals("sequence") : sequence;
		
		this.control.reset();
		this.control.replay();
		
		sequence = this.dialect.parseSequence("SELECT * FROM table");
		
		this.control.verify();
		
		assert sequence == null : sequence;
		
		return sequence;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#getDefaultSchemas(java.sql.Connection)
	 */
	@Test(dataProvider = "connection")
	public List<String> getDefaultSchemas(Connection connection) throws SQLException
	{
		EasyMock.expect(connection.createStatement()).andReturn(this.statement);
		EasyMock.expect(this.statement.executeQuery("SELECT CURRENT_USER")).andReturn(this.resultSet);
		EasyMock.expect(this.resultSet.next()).andReturn(false);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("user");

		this.resultSet.close();
		this.statement.close();
		
		this.control.replay();
		
		List<String> schemaList = this.dialect.getDefaultSchemas(connection);
		
		this.control.verify();
		
		assert schemaList.size() == 1 : schemaList.size();
		
		assert schemaList.get(0).equals("user") : schemaList.get(0);
		
		return schemaList;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#isAutoIncrementing(net.sf.hajdbc.ColumnProperties)
	 */
	@Test(dataProvider = "column")
	public boolean isAutoIncrementing(ColumnProperties properties) throws SQLException
	{
		EasyMock.expect(properties.getRemarks()).andReturn("GENERATED BY DEFAULT AS IDENTITY");
		
		this.control.replay();
		
		boolean autoIncrementing = this.dialect.isAutoIncrementing(properties);
		
		this.control.verify();
		this.control.reset();
		
		EasyMock.expect(this.columnProperties.getRemarks()).andReturn(null);
		
		this.control.replay();
		
		autoIncrementing = this.dialect.isAutoIncrementing(properties);
		
		this.control.verify();
		
		return autoIncrementing;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#parseInsertTable(java.lang.String)
	 */
	@Test(dataProvider = "null")
	public String parseInsertTable(String sql) throws SQLException
	{
		this.control.replay();
		
		String table = this.dialect.parseInsertTable("INSERT INTO test VALUES (...)");

		this.control.verify();
		
		assert table != null;
		assert table.equals("test") : table;
		
		this.control.reset();
		this.control.replay();
		
		table = this.dialect.parseInsertTable("SELECT * FROM test WHERE ...");
		
		this.control.verify();

		assert table == null : table;

		return table;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#supportsAutoIncrementColumns()
	 */
	@Test
	public boolean supportsAutoIncrementColumns()
	{
		this.control.replay();
		
		boolean supports = this.dialect.supportsAutoIncrementColumns();
		
		this.control.verify();
		
		assert supports;
		
		return supports;
	}

	/**
	 * @see net.sf.hajdbc.Dialect#supportsSequences()
	 */
	@Test
	public boolean supportsSequences()
	{
		this.control.replay();
		
		boolean supports = this.dialect.supportsSequences();
		
		this.control.verify();
		
		assert supports;
		
		return supports;
	}
}
