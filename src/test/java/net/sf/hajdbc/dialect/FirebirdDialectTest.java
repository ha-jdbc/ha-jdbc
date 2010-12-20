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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Map;

import net.sf.hajdbc.SequenceSupport;
import net.sf.hajdbc.cache.QualifiedName;
import net.sf.hajdbc.cache.SequenceProperties;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.Assert;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
public class FirebirdDialectTest extends StandardDialectTest
{
	public FirebirdDialectTest()
	{
		super(DialectFactoryEnum.FIREBIRD);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getSequenceSupport()
	 */
	@Override
	public void getSequenceSupport()
	{
		Assert.assertSame(this.dialect, this.dialect.getSequenceSupport());
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getAlterSequenceSQL()
	 */
	@Override
	public void getAlterSequenceSQL() throws SQLException
	{
		SequenceProperties sequence = EasyMock.createStrictMock(SequenceProperties.class);
		
		EasyMock.expect(sequence.getName()).andReturn("sequence");
		EasyMock.expect(sequence.getIncrement()).andReturn(1);
		
		EasyMock.replay(sequence);
		
		String result = this.dialect.getSequenceSupport().getAlterSequenceSQL(sequence, 1000L);
		
		EasyMock.verify(sequence);

		Assert.assertEquals("SET GENERATOR sequence TO 1000", result);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getSequences()
	 */
	@Override
	public void getSequences() throws SQLException
	{
		IMocksControl control = EasyMock.createStrictControl();
		DatabaseMetaData metaData = control.createMock(DatabaseMetaData.class);
		Connection connection = control.createMock(Connection.class);
		Statement statement = control.createMock(Statement.class);
		ResultSet resultSet = control.createMock(ResultSet.class);
		
		EasyMock.expect(metaData.getConnection()).andReturn(connection);
		EasyMock.expect(connection.createStatement()).andReturn(statement);
		EasyMock.expect(statement.executeQuery("SELECT RDB$GENERATOR_NAME FROM RDB$GENERATORS")).andReturn(resultSet);
		EasyMock.expect(resultSet.next()).andReturn(true);
		EasyMock.expect(resultSet.getString(1)).andReturn("sequence1");
		EasyMock.expect(resultSet.next()).andReturn(true);
		EasyMock.expect(resultSet.getString(1)).andReturn("sequence2");
		EasyMock.expect(resultSet.next()).andReturn(false);
		
		statement.close();
		
		control.replay();
		
		Map<QualifiedName, Integer> results = this.dialect.getSequenceSupport().getSequences(metaData);

		control.verify();
		
		Assert.assertEquals(results.size(), 2);
		
		Iterator<Map.Entry<QualifiedName, Integer>> entries = results.entrySet().iterator();
		Map.Entry<QualifiedName, Integer> entry = entries.next();

		Assert.assertNull(entry.getKey().getSchema());
		Assert.assertEquals("sequence1", entry.getKey().getName());
		Assert.assertEquals(1, entry.getValue().intValue());
		
		entry = entries.next();
		
		Assert.assertNull(entry.getKey().getSchema());
		Assert.assertEquals("sequence2", entry.getKey().getName());
		Assert.assertEquals(1, entry.getValue().intValue());
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getNextSequenceValueSQL()
	 */
	@Override
	public void getNextSequenceValueSQL() throws SQLException
	{
		SequenceProperties sequence = EasyMock.createStrictMock(SequenceProperties.class);
		
		EasyMock.expect(sequence.getName()).andReturn("sequence");
		
		EasyMock.replay(sequence);
		
		String result = this.dialect.getSequenceSupport().getNextSequenceValueSQL(sequence);
		
		EasyMock.verify(sequence);
		
		Assert.assertEquals("SELECT GEN_ID(sequence, 1) FROM RDB$DATABASE", result);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#getSimpleSQL()
	 */
	@Override
	public void getSimpleSQL() throws SQLException
	{
		Assert.assertEquals("SELECT CURRENT_TIMESTAMP FROM RDB$DATABASE", this.dialect.getSimpleSQL());
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#isSelectForUpdate()
	 */
	@Override
	public void isSelectForUpdate() throws SQLException
	{
		Assert.assertTrue(this.dialect.isSelectForUpdate("SELECT * FROM test WITH LOCK"));
		Assert.assertFalse(this.dialect.isSelectForUpdate("SELECT * FROM test"));
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.dialect.StandardDialectTest#parseSequence()
	 */
	@Override
	public void parseSequence() throws SQLException
	{
		SequenceSupport support = this.dialect.getSequenceSupport();
		Assert.assertEquals("sequence", support.parseSequence("SELECT GEN_ID(sequence, 1) FROM RDB$DATABASE"));
		Assert.assertEquals("sequence", support.parseSequence("SELECT GEN_ID(sequence, 1), * FROM table"));
		Assert.assertEquals("sequence", support.parseSequence("INSERT INTO table VALUES (GEN_ID(sequence, 1), 0)"));
		Assert.assertEquals("sequence", support.parseSequence("UPDATE table SET id = GEN_ID(sequence, 1)"));
		Assert.assertNull(support.parseSequence("SELECT NEXT VALUE FOR test"));
		Assert.assertNull(support.parseSequence("SELECT NEXT VALUE FOR test, * FROM table"));
		Assert.assertNull(support.parseSequence("INSERT INTO table VALUES (NEXT VALUE FOR test)"));
		Assert.assertNull(support.parseSequence("UPDATE table SET id = NEXT VALUE FOR test"));
		Assert.assertNull(support.parseSequence("SELECT * FROM table"));
	}
}
