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
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.SequenceProperties;

import org.easymock.EasyMock;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
public class TestDB2Dialect extends TestStandardDialect
{
	@Override
	protected Dialect createDialect()
	{
		return new DB2Dialect();
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getSequences(java.sql.DatabaseMetaData)
	 */
	@Override
	@Test(dataProvider = "meta-data")
	public Collection<QualifiedName> getSequences(DatabaseMetaData metaData) throws SQLException
	{
		EasyMock.expect(metaData.getConnection()).andReturn(this.connection);
		EasyMock.expect(this.connection.createStatement()).andReturn(this.statement);
		EasyMock.expect(this.statement.executeQuery("SELECT SEQSCHEMA, SEQNAME FROM SYSCAT.SEQUENCES")).andReturn(this.resultSet);
		EasyMock.expect(this.resultSet.next()).andReturn(true);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("schema1");
		EasyMock.expect(this.resultSet.getString(2)).andReturn("sequence1");
		EasyMock.expect(this.resultSet.next()).andReturn(true);
		EasyMock.expect(this.resultSet.getString(1)).andReturn("schema2");
		EasyMock.expect(this.resultSet.getString(2)).andReturn("sequence2");
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
		
		assert schema.equals("schema1") : schema;
		assert name.equals("sequence1") : name;
		
		sequence = iterator.next();
		schema = sequence.getSchema();
		name = sequence.getName();
		
		assert schema.equals("schema2") : schema;
		assert name.equals("sequence2") : name;
		
		return sequences;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getCurrentSequenceValueSQL(java.lang.String)
	 */
	@Override
	@Test(dataProvider = "sequence")
	public String getNextSequenceValueSQL(SequenceProperties sequence) throws SQLException
	{
		EasyMock.expect(sequence.getName()).andReturn("sequence");
		
		this.replay();
		
		String sql = this.dialect.getNextSequenceValueSQL(sequence);
		
		this.verify();
		
		assert sql.equals("VALUES NEXTVAL FOR sequence") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#getSimpleSQL()
	 */
	@Override
	public String getSimpleSQL() throws SQLException
	{
		this.replay();
		
		String sql = this.dialect.getSimpleSQL();

		this.verify();
		
		assert sql.equals("VALUES CURRENT_TIMESTAMP") : sql;
		
		return sql;
	}

	/**
	 * @see net.sf.hajdbc.dialect.TestStandardDialect#parseSequence(java.lang.String)
	 */
	@Override
	@Test(dataProvider = "null")
	public String parseSequence(String sql) throws SQLException
	{
		this.replay();
		
		String sequence = this.dialect.parseSequence("VALUES NEXTVAL FOR sequence");
		
		this.verify();
		
		assert sequence.equals("sequence") : sequence;
		
		this.replay();
		
		sequence = this.dialect.parseSequence("VALUES PREVVAL FOR sequence");
		
		this.verify();
		
		assert sequence.equals("sequence") : sequence;
		
		this.replay();
		
		sequence = this.dialect.parseSequence("SELECT * FROM table");
		
		this.verify();
		
		assert sequence == null : sequence;
		
		return sequence;
	}
	
	@Override
	@Test(dataProvider = "current-date")
	public String evaluateCurrentDate(String sql, java.sql.Date date)
	{
		String expected = sql.contains("success") ? "SELECT '" + date.toString() + "' FROM success" : sql;
		
		String evaluated = this.dialect.evaluateCurrentDate(sql, date);

		assert evaluated.equals(expected) : evaluated;
		
		return evaluated;
	}
	
	@Override
	@Test(dataProvider = "current-time")
	public String evaluateCurrentTime(String sql, java.sql.Time date)
	{
		String expected = sql.contains("success") ? "SELECT '" + date.toString() + "' FROM success" : sql;
		
		String evaluated = this.dialect.evaluateCurrentTime(sql, date);

		assert evaluated.equals(expected) : evaluated;
		
		return evaluated;
	}
	
	@Override
	@Test(dataProvider = "current-timestamp")
	public String evaluateCurrentTimestamp(String sql, java.sql.Timestamp date)
	{
		String expected = sql.contains("success") ? "SELECT '" + date.toString() + "' FROM success" : sql;
		
		String evaluated = this.dialect.evaluateCurrentTimestamp(sql, date);

		assert evaluated.equals(expected) : evaluated;
		
		return evaluated;
	}
}
