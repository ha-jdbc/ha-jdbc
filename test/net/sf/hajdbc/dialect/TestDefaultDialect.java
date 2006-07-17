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
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.UniqueConstraint;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.testng.annotations.Configuration;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
public class TestDefaultDialect implements Dialect
{
	protected IMocksControl control = EasyMock.createStrictControl();
	protected TableProperties tableProperties = this.control.createMock(TableProperties.class);
	protected Connection connection = this.control.createMock(Connection.class);
	
	protected Dialect dialect = this.createDialect();
	
	protected Dialect createDialect()
	{
		return new DefaultDialect();
	}
	
	@Configuration(afterTestMethod = true)
	public void reset()
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
		ForeignKeyConstraint foreignKey = new ForeignKeyConstraint("name", "table");
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
		UniqueConstraint uniqueKey = new UniqueConstraint("name", "table");
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
		return new Object[][] { new Object[] { new ColumnProperties("column", Types.INTEGER, "int") } };
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

	/**
	 * @see net.sf.hajdbc.Dialect#getAlterSequenceSQL(java.lang.String, long)
	 */
	@Test(dataProvider = "alter-sequence")
	public String getAlterSequenceSQL(String sequence, long value)
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
		this.control.replay();
		
		int type = this.dialect.getColumnType(properties);
		
		this.control.verify();
		
		assert type == properties.getType() : type;
		
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
	 * @see net.sf.hajdbc.Dialect#getCreatePrimaryKeyConstraintSQL(net.sf.hajdbc.UniqueConstraint)
	 */
	@Test(dataProvider = "unique-constraint")
	public String getCreatePrimaryKeyConstraintSQL(UniqueConstraint constraint) throws SQLException
	{
		this.control.replay();
		
		String sql = this.dialect.getCreatePrimaryKeyConstraintSQL(constraint);
		
		assert sql.equals("ALTER TABLE table ADD CONSTRAINT name PRIMARY KEY (column1, column2)") : sql;
		
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
	 * @see net.sf.hajdbc.Dialect#getDropPrimaryKeyConstraintSQL(net.sf.hajdbc.UniqueConstraint)
	 */
	@Test(dataProvider = "unique-constraint")
	public String getDropPrimaryKeyConstraintSQL(UniqueConstraint constraint) throws SQLException
	{
		this.control.replay();
		
		String sql = this.dialect.getDropPrimaryKeyConstraintSQL(constraint);
		
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
		UniqueConstraint primaryKey = new UniqueConstraint("name", "table");
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
	 * @see net.sf.hajdbc.Dialect#getSequences(java.sql.Connection)
	 */
	@Test(dataProvider = "connection")
	public Map<String, Long> getSequences(Connection connection) throws SQLException
	{
		this.control.replay();
		
		Map<String, Long> sequenceMap = this.dialect.getSequences(connection);
		
		this.control.verify();
		
		assert sequenceMap.isEmpty() : sequenceMap;
		
		return sequenceMap;
	}
	
	/**
	 * @see net.sf.hajdbc.Dialect#getSimpleSQL()
	 */
	@Test
	public String getSimpleSQL()
	{
		this.control.replay();
		
		String sql = this.dialect.getSimpleSQL();
		
		this.control.verify();
		
		assert sql.equals("SELECT 1") : sql;
		
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
}
