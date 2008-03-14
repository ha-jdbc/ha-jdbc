/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.TableProperties;

import org.easymock.EasyMock;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
public class TestMaxDBDialect extends TestStandardDialect
{
	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#createDialect()
	 */
	@Override
	protected Dialect createDialect()
	{
		return new MaxDBDialect();
	}

	@Override
	@Test(dataProvider = "foreign-key")
	public String getCreateForeignKeyConstraintSQL(ForeignKeyConstraint constraint) throws SQLException
	{
		this.replay();
		
		String sql = this.dialect.getCreateForeignKeyConstraintSQL(constraint);

		this.verify();
		
		assert sql.equals("ALTER TABLE table ADD CONSTRAINT name FOREIGN KEY (column1, column2) REFERENCES foreign_table (foreign_column1, foreign_column2) ON DELETE CASCADE") : sql;
		
		return sql;
	}

	@Override
	@Test(dataProvider = "meta-data")
	public Collection<QualifiedName> getSequences(DatabaseMetaData metaData) throws SQLException
	{
		EasyMock.expect(metaData.getConnection()).andReturn(this.connection);
		EasyMock.expect(this.connection.createStatement()).andReturn(this.statement);
		EasyMock.expect(this.statement.executeQuery("SELECT SEQUENCE_NAME FROM USER_SEQUENCES")).andReturn(this.resultSet);
		EasyMock.expect(this.resultSet.next()).andReturn(true);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("sequence1");
		EasyMock.expect(this.resultSet.next()).andReturn(true);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("sequence2");
		EasyMock.expect(this.resultSet.next()).andReturn(false);
		
		this.statement.close();
		
		this.replay();
		
		Collection<QualifiedName> sequences = this.dialect.getSequences(metaData);
		
		this.verify();
		
		assert sequences.size() == 2 : sequences.size();
		
		Iterator<QualifiedName> iterator = sequences.iterator();
		QualifiedName sequence = iterator.next();
		String schema = sequence.getSchema();
		String name = sequence.getName();
		
		assert schema == null : schema;
		assert name.equals("sequence1") : name;
		
		sequence = iterator.next();
		schema = sequence.getSchema();
		name = sequence.getName();
		
		assert schema == null : schema;
		assert name.equals("sequence2") : name;
		
		return sequences;
	}

	@Override
	@Test
	public String getSimpleSQL() throws SQLException
	{
		this.replay();
		
		String sql = this.dialect.getSimpleSQL();
		
		this.verify();
		
		assert sql.equals("SELECT SYSDATE FROM DUAL") : sql;
		
		return sql;
	}

	@Override
	@Test(dataProvider = "table")
	public String getTruncateTableSQL(TableProperties properties) throws SQLException
	{
		EasyMock.expect(properties.getName()).andReturn("table");
		
		this.replay();
		
		String sql = this.dialect.getTruncateTableSQL(properties);
		
		this.verify();
		
		assert sql.equals("TRUNCATE TABLE table");
		
		return sql;
	}

	@Override
	@DataProvider(name = "sequence-sql")
	Object[][] sequenceSQLProvider()
	{
		return new Object[][] {
			new Object[] { "SELECT success.nextval" },
			new Object[] { "SELECT success.currval" },
			new Object[] { "SELECT success.nextval, * FROM table" },
			new Object[] { "SELECT success.currval, * FROM table" },
			new Object[] { "INSERT INTO table VALUES (success.nextval, 0)" },
			new Object[] { "INSERT INTO table VALUES (success.currval, 0)" },
			new Object[] { "UPDATE table SET id = success.nextval" },
			new Object[] { "UPDATE table SET id = success.currval" },
			new Object[] { "SELECT * FROM table" },
		};
	}
	
	@Override
	@Test(dataProvider = "insert-table-sql")
	public String parseInsertTable(String sql) throws SQLException
	{
		this.replay();
		
		String table = this.dialect.parseInsertTable(sql);
		
		this.verify();

		assert (table == null) : table;

		return table;
	}
	
	@Override
	@Test(dataProvider = "sequence")
	public String getNextSequenceValueSQL(SequenceProperties sequence) throws SQLException
	{
		EasyMock.expect(sequence.getName()).andReturn("sequence");
		
		this.replay();
		
		String sql = this.dialect.getNextSequenceValueSQL(sequence);
		
		this.verify();
		
		assert sql.equals("SELECT sequence.NEXTVAL FROM DUAL") : sql;
		
		return sql;
	}
}
