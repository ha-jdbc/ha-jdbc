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

import java.sql.BatchUpdateException;
import java.sql.DataTruncation;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;
import java.sql.SQLWarning;
import java.sql.Types;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.sql.rowset.RowSetWarning;
import javax.sql.rowset.serial.SerialException;
import javax.sql.rowset.spi.SyncFactoryException;
import javax.sql.rowset.spi.SyncProviderException;
import javax.transaction.xa.XAException;

import junit.framework.Assert;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.IdentityColumnSupport;
import net.sf.hajdbc.SequenceSupport;
import net.sf.hajdbc.cache.ColumnProperties;
import net.sf.hajdbc.cache.ForeignKeyConstraint;
import net.sf.hajdbc.cache.ForeignKeyConstraintImpl;
import net.sf.hajdbc.cache.QualifiedName;
import net.sf.hajdbc.cache.SequenceProperties;
import net.sf.hajdbc.cache.TableProperties;
import net.sf.hajdbc.cache.UniqueConstraint;
import net.sf.hajdbc.cache.UniqueConstraintImpl;

import org.easymock.EasyMock;
import org.junit.Test;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("nls")
public class StandardDialectTest
{
 	Dialect dialect;
 	
	public StandardDialectTest()
	{
		this(DialectFactoryEnum.STANDARD);
	}
	
	protected StandardDialectTest(DialectFactory dialectFactory)
	{
		this.dialect = dialectFactory.createDialect();
	}

	@Test
	public void getSequenceSupport()
	{
		Assert.assertNull(this.dialect.getSequenceSupport());
	}
	
	@Test
	public void getIdentityColumnSupport()
	{
		Assert.assertNull(this.dialect.getIdentityColumnSupport());
	}
	
	@Test
	public void getAlterSequenceSQL() throws SQLException
	{
		SequenceSupport support = this.dialect.getSequenceSupport();
		
		if (support != null)
		{
			SequenceProperties sequence = EasyMock.createStrictMock(SequenceProperties.class);
			
			EasyMock.expect(sequence.getName()).andReturn("sequence");
			EasyMock.expect(sequence.getIncrement()).andReturn(1);
			
			EasyMock.replay(sequence);
			
			String result = support.getAlterSequenceSQL(sequence, 1000L);

			EasyMock.verify(sequence);
			
			Assert.assertEquals("ALTER SEQUENCE sequence RESTART WITH 1000", result);
		}
	}

	@Test
	public void getColumnType() throws SQLException
	{
		ColumnProperties column = EasyMock.createStrictMock(ColumnProperties.class);
		
		EasyMock.expect(column.getType()).andReturn(Types.INTEGER);
		
		EasyMock.replay(column);
		
		int result = this.dialect.getColumnType(column);
		
		EasyMock.verify(column);
		
		Assert.assertEquals(Types.INTEGER, result);
	}

	@Test
	public void getCreateForeignKeyConstraintSQL() throws SQLException
	{
		ForeignKeyConstraint key = new ForeignKeyConstraintImpl("name", "table");
		key.getColumnList().add("column1");
		key.getColumnList().add("column2");
		key.setForeignTable("foreign_table");
		key.getForeignColumnList().add("foreign_column1");
		key.getForeignColumnList().add("foreign_column2");
		key.setDeferrability(DatabaseMetaData.importedKeyInitiallyDeferred);
		key.setDeleteRule(DatabaseMetaData.importedKeyCascade);
		key.setUpdateRule(DatabaseMetaData.importedKeyRestrict);
		
		String result = this.dialect.getCreateForeignKeyConstraintSQL(key);
		
		Assert.assertEquals("ALTER TABLE table ADD CONSTRAINT name FOREIGN KEY (column1, column2) REFERENCES foreign_table (foreign_column1, foreign_column2) ON DELETE CASCADE ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED", result);
	}

	@Test
	public void getCreateUniqueConstraintSQL() throws SQLException
	{
		UniqueConstraint key = new UniqueConstraintImpl("name", "table");
		key.getColumnList().add("column1");
		key.getColumnList().add("column2");
		
		String result = this.dialect.getCreateUniqueConstraintSQL(key);
		
		Assert.assertEquals("ALTER TABLE table ADD CONSTRAINT name UNIQUE (column1, column2)", result);
	}

	@Test
	public void getDropForeignKeyConstraintSQL() throws SQLException
	{
		ForeignKeyConstraint key = new ForeignKeyConstraintImpl("name", "table");
		key.getColumnList().add("column1");
		key.getColumnList().add("column2");
		key.setForeignTable("foreign_table");
		key.getForeignColumnList().add("foreign_column1");
		key.getForeignColumnList().add("foreign_column2");
		key.setDeferrability(DatabaseMetaData.importedKeyInitiallyDeferred);
		key.setDeleteRule(DatabaseMetaData.importedKeyCascade);
		key.setUpdateRule(DatabaseMetaData.importedKeyRestrict);
		
		String result = this.dialect.getDropForeignKeyConstraintSQL(key);
		
		Assert.assertEquals("ALTER TABLE table DROP CONSTRAINT name", result);
	}

	@Test
	public void getDropUniqueConstraintSQL() throws SQLException
	{
		UniqueConstraint key = new UniqueConstraintImpl("name", "table");
		key.getColumnList().add("column1");
		key.getColumnList().add("column2");
		
		String result = this.dialect.getDropUniqueConstraintSQL(key);
		
		Assert.assertEquals("ALTER TABLE table DROP CONSTRAINT name", result);
	}

	@Test
	public void getNextSequenceValueSQL() throws SQLException
	{
		SequenceSupport support = this.dialect.getSequenceSupport();
		
		if (support != null)
		{
			SequenceProperties sequence = EasyMock.createStrictMock(SequenceProperties.class);
			
			EasyMock.expect(sequence.getName()).andReturn("sequence");
			
			EasyMock.replay(sequence);
			
			String result = support.getNextSequenceValueSQL(sequence);
			
			EasyMock.verify(sequence);
			
			Assert.assertEquals("SELECT NEXT VALUE FOR sequence", result);
		}
	}

	@Test
	public void getSequences() throws SQLException
	{
		SequenceSupport support = this.dialect.getSequenceSupport();
		
		if (support != null)
		{
			DatabaseMetaData metaData = EasyMock.createStrictMock(DatabaseMetaData.class);
			ResultSet resultSet = EasyMock.createStrictMock(ResultSet.class);
			
			EasyMock.expect(metaData.getTables(EasyMock.eq(""), EasyMock.eq((String) null), EasyMock.eq("%"), EasyMock.aryEq(new String[] { "SEQUENCE" }))).andReturn(resultSet);
			EasyMock.expect(resultSet.next()).andReturn(true);
			EasyMock.expect(resultSet.getString("TABLE_SCHEM")).andReturn("schema1");
			EasyMock.expect(resultSet.getString("TABLE_NAME")).andReturn("sequence1");
			EasyMock.expect(resultSet.next()).andReturn(true);
			EasyMock.expect(resultSet.getString("TABLE_SCHEM")).andReturn("schema2");
			EasyMock.expect(resultSet.getString("TABLE_NAME")).andReturn("sequence2");
			EasyMock.expect(resultSet.next()).andReturn(false);
			
			resultSet.close();
			
			EasyMock.replay(metaData, resultSet);
			
			Map<QualifiedName, Integer> results = support.getSequences(metaData);
			
			EasyMock.verify(metaData, resultSet);
			
			Assert.assertEquals(2, results.size());
			
			Iterator<Map.Entry<QualifiedName, Integer>> iterator = results.entrySet().iterator();
			Map.Entry<QualifiedName, Integer> sequence = iterator.next();
	
			Assert.assertEquals("schema1", sequence.getKey().getSchema());
			Assert.assertEquals("sequence1", sequence.getKey().getName());
			Assert.assertEquals(1, sequence.getValue().intValue());
			
			sequence = iterator.next();
	
			Assert.assertEquals("schema2", sequence.getKey().getSchema());
			Assert.assertEquals("sequence2", sequence.getKey().getName());
			Assert.assertEquals(1, sequence.getValue().intValue());
		}
	}

	@Test
	public void getSimpleSQL() throws SQLException
	{
		String result = this.dialect.getSimpleSQL();

		Assert.assertEquals("SELECT CURRENT_TIMESTAMP", result);
	}

	@Test
	public void getTruncateTableSQL() throws SQLException
	{
		TableProperties table = EasyMock.createStrictMock(TableProperties.class);
		
		EasyMock.expect(table.getName()).andReturn("table");
		
		EasyMock.replay(table);
		
		String result = this.dialect.getTruncateTableSQL(table);
		
		EasyMock.verify(table);

		Assert.assertEquals("DELETE FROM table", result);
	}

	@Test
	public void isSelectForUpdate() throws SQLException
	{
		Assert.assertTrue(this.dialect.isSelectForUpdate("SELECT * FROM test FOR UPDATE"));
		Assert.assertFalse(this.dialect.isSelectForUpdate("SELECT * FROM test"));
	}

	@Test
	public void parseSequence() throws SQLException
	{
		SequenceSupport support = this.dialect.getSequenceSupport();
		
		if (support != null)
		{
			Assert.assertEquals("test", support.parseSequence("SELECT NEXT VALUE FOR test"));
			Assert.assertEquals("test", support.parseSequence("SELECT NEXT VALUE FOR test, * FROM table"));
			Assert.assertEquals("test", support.parseSequence("INSERT INTO table VALUES (NEXT VALUE FOR test)"));
			Assert.assertEquals("test", support.parseSequence("UPDATE table SET id = NEXT VALUE FOR test"));
			Assert.assertNull(support.parseSequence("SELECT * FROM table"));
		}
	}

	@Test
	public void getDefaultSchemas() throws SQLException
	{
		DatabaseMetaData metaData = EasyMock.createStrictMock(DatabaseMetaData.class);
		
		String user = "user";
		
		EasyMock.expect(metaData.getUserName()).andReturn(user);
		
		EasyMock.replay(metaData);
		
		List<String> result = this.dialect.getDefaultSchemas(metaData);
		
		EasyMock.verify(metaData);

		Assert.assertEquals(1, result.size());
		Assert.assertSame(user, result.get(0));
	}

	@Test
	public void parseInsertTable() throws SQLException
	{
		IdentityColumnSupport support = this.dialect.getIdentityColumnSupport();
		
		if (support != null)
		{
			Assert.assertEquals("table", support.parseInsertTable("INSERT INTO table (column1, column2) VALUES (1, 2)"));
			Assert.assertEquals("table", support.parseInsertTable("INSERT INTO table VALUES (1, 2)"));
			Assert.assertEquals("table", support.parseInsertTable("INSERT table (column1, column2) VALUES (1, 2)"));
			Assert.assertEquals("table", support.parseInsertTable("INSERT table VALUES (1, 2)"));
			Assert.assertEquals("table", support.parseInsertTable("INSERT INTO table (column1, column2) SELECT column1, column2 FROM dummy"));
			Assert.assertEquals("table", support.parseInsertTable("INSERT INTO table SELECT column1, column2 FROM dummy"));
			Assert.assertEquals("table", support.parseInsertTable("INSERT table (column1, column2) SELECT column1, column2 FROM dummy"));
			Assert.assertEquals("table", support.parseInsertTable("INSERT table SELECT column1, column2 FROM dummy"));
			Assert.assertNull(support.parseInsertTable("SELECT * FROM table WHERE 0=1"));
			Assert.assertNull(support.parseInsertTable("UPDATE table SET column = 0"));
		}
	}
	
	@Test
	public void getIdentifierPattern() throws SQLException
	{
		DatabaseMetaData metaData = EasyMock.createStrictMock(DatabaseMetaData.class);
		
		EasyMock.expect(metaData.getExtraNameCharacters()).andReturn("$");
		
		EasyMock.replay(metaData);
		
		String result = this.dialect.getIdentifierPattern(metaData).pattern();
		
		EasyMock.verify(metaData);
		
		Assert.assertEquals("[a-zA-Z][\\w\\Q$\\E]*", result);
	}

	@Test
	public void evaluateCurrentDate()
	{
		java.sql.Date date = new java.sql.Date(System.currentTimeMillis());
		
		Assert.assertEquals(String.format("SELECT DATE '%s' FROM test", date.toString()), this.dialect.evaluateCurrentDate("SELECT CURRENT_DATE FROM test", date));
		Assert.assertEquals("SELECT CCURRENT_DATE FROM test", this.dialect.evaluateCurrentDate("SELECT CCURRENT_DATE FROM test", date));
		Assert.assertEquals("SELECT CURRENT_DATES FROM test", this.dialect.evaluateCurrentDate("SELECT CURRENT_DATES FROM test", date));
		Assert.assertEquals("SELECT CURRENT_TIME FROM test", this.dialect.evaluateCurrentDate("SELECT CURRENT_TIME FROM test", date));
		Assert.assertEquals("SELECT CURRENT_TIMESTAMP FROM test", this.dialect.evaluateCurrentDate("SELECT CURRENT_TIMESTAMP FROM test", date));
	}

	@Test
	public void evaluateCurrentTime()
	{
		java.sql.Time time = new java.sql.Time(System.currentTimeMillis());
		
		Assert.assertEquals(String.format("SELECT TIME '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT CURRENT_TIME FROM test", time));
		Assert.assertEquals(String.format("SELECT TIME '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT CURRENT_TIME(2) FROM test", time));
		Assert.assertEquals(String.format("SELECT TIME '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT CURRENT_TIME ( 2 ) FROM test", time));
		Assert.assertEquals(String.format("SELECT TIME '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT LOCALTIME FROM test", time));
		Assert.assertEquals(String.format("SELECT TIME '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT LOCALTIME(2) FROM test", time));
		Assert.assertEquals(String.format("SELECT TIME '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT LOCALTIME ( 2 ) FROM test", time));
		Assert.assertEquals("SELECT CCURRENT_TIME FROM test", this.dialect.evaluateCurrentTime("SELECT CCURRENT_TIME FROM test", time));
		Assert.assertEquals("SELECT LLOCALTIME FROM test", this.dialect.evaluateCurrentTime("SELECT LLOCALTIME FROM test", time));
		Assert.assertEquals("SELECT CURRENT_DATE FROM test", this.dialect.evaluateCurrentTime("SELECT CURRENT_DATE FROM test", time));
		Assert.assertEquals("SELECT CURRENT_TIMESTAMP FROM test", this.dialect.evaluateCurrentTime("SELECT CURRENT_TIMESTAMP FROM test", time));
		Assert.assertEquals("SELECT LOCALTIMESTAMP FROM test", this.dialect.evaluateCurrentTime("SELECT LOCALTIMESTAMP FROM test", time));
	}

	@Test
	public void evaluateCurrentTimestamp()
	{
		java.sql.Timestamp timestamp = new java.sql.Timestamp(System.currentTimeMillis());
		
		Assert.assertEquals(String.format("SELECT TIMESTAMP '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_TIMESTAMP FROM test", timestamp));
		Assert.assertEquals(String.format("SELECT TIMESTAMP '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_TIMESTAMP(2) FROM test", timestamp));
		Assert.assertEquals(String.format("SELECT TIMESTAMP '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_TIMESTAMP ( 2 ) FROM test", timestamp));
		Assert.assertEquals(String.format("SELECT TIMESTAMP '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT LOCALTIMESTAMP FROM test", timestamp));
		Assert.assertEquals(String.format("SELECT TIMESTAMP '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT LOCALTIMESTAMP(2) FROM test", timestamp));
		Assert.assertEquals(String.format("SELECT TIMESTAMP '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT LOCALTIMESTAMP ( 2 ) FROM test", timestamp));
		Assert.assertEquals("SELECT CCURRENT_TIMESTAMP FROM test", this.dialect.evaluateCurrentTimestamp("SELECT CCURRENT_TIMESTAMP FROM test", timestamp));
		Assert.assertEquals("SELECT LLOCALTIMESTAMP FROM test", this.dialect.evaluateCurrentTimestamp("SELECT LLOCALTIMESTAMP FROM test", timestamp));
		Assert.assertEquals("SELECT CURRENT_DATE FROM test", this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_DATE FROM test", timestamp));
		Assert.assertEquals("SELECT CURRENT_TIME FROM test", this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_TIME FROM test", timestamp));
		Assert.assertEquals("SELECT LOCALTIME FROM test", this.dialect.evaluateCurrentTimestamp("SELECT LOCALTIME FROM test", timestamp));
	}

	@Test
	public void evaluateRand()
	{
		Assert.assertTrue(Pattern.matches("SELECT ((0\\.\\d+)|([1-9]\\.\\d+E\\-\\d+)) FROM test", this.dialect.evaluateRand("SELECT RAND() FROM test")));
		Assert.assertTrue(Pattern.matches("SELECT ((0\\.\\d+)|([1-9]\\.\\d+E\\-\\d+)) FROM test", this.dialect.evaluateRand("SELECT RAND ( ) FROM test")));
		Assert.assertEquals("SELECT RAND FROM test", this.dialect.evaluateRand("SELECT RAND FROM test"));
		Assert.assertEquals("SELECT OPERAND() FROM test", this.dialect.evaluateRand("SELECT OPERAND() FROM test"));
		Assert.assertEquals("SELECT 1 FROM test", this.dialect.evaluateRand("SELECT 1 FROM test"));
	}

	@Test
	public void getAlterIdentityColumnSQL() throws SQLException
	{
		IdentityColumnSupport support = this.dialect.getIdentityColumnSupport();
		
		if (support != null)
		{
			TableProperties table = EasyMock.createStrictMock(TableProperties.class);
			ColumnProperties column = EasyMock.createStrictMock(ColumnProperties.class);
			
			EasyMock.expect(table.getName()).andReturn("table");
			EasyMock.expect(column.getName()).andReturn("column");
			
			EasyMock.replay(table, column);
			
			String result = support.getAlterIdentityColumnSQL(table, column, 1000L);
			
			EasyMock.verify(table, column);
			
			Assert.assertEquals("ALTER TABLE table ALTER COLUMN column RESTART WITH 1000", result);
		}
	}
	
	@Test
	public void indicatesFailureSQLException()
	{
		Assert.assertTrue(this.dialect.indicatesFailure(new SQLNonTransientConnectionException()));
		Assert.assertFalse(this.dialect.indicatesFailure(new SQLException()));
		Assert.assertFalse(this.dialect.indicatesFailure(new BatchUpdateException()));
		Assert.assertFalse(this.dialect.indicatesFailure(new RowSetWarning()));
		Assert.assertFalse(this.dialect.indicatesFailure(new SerialException()));
		Assert.assertFalse(this.dialect.indicatesFailure(new SQLClientInfoException()));
		Assert.assertFalse(this.dialect.indicatesFailure(new SQLNonTransientException()));
		Assert.assertFalse(this.dialect.indicatesFailure(new SQLDataException()));
		Assert.assertFalse(this.dialect.indicatesFailure(new SQLFeatureNotSupportedException()));
		Assert.assertFalse(this.dialect.indicatesFailure(new SQLIntegrityConstraintViolationException()));
		Assert.assertFalse(this.dialect.indicatesFailure(new SQLInvalidAuthorizationSpecException()));
		Assert.assertFalse(this.dialect.indicatesFailure(new SQLSyntaxErrorException()));
		Assert.assertFalse(this.dialect.indicatesFailure(new SQLRecoverableException()));
		Assert.assertFalse(this.dialect.indicatesFailure(new SQLTransientException()));
		Assert.assertFalse(this.dialect.indicatesFailure(new SQLTimeoutException()));
		Assert.assertFalse(this.dialect.indicatesFailure(new SQLTransactionRollbackException()));
		Assert.assertFalse(this.dialect.indicatesFailure(new SQLTransientConnectionException()));
		Assert.assertFalse(this.dialect.indicatesFailure(new SQLWarning()));
		Assert.assertFalse(this.dialect.indicatesFailure(new DataTruncation(1, false, false, 1, 1)));
		Assert.assertFalse(this.dialect.indicatesFailure(new SQLDataException()));
		Assert.assertFalse(this.dialect.indicatesFailure(new SyncFactoryException()));
		Assert.assertFalse(this.dialect.indicatesFailure(new SyncProviderException()));
	}
	
	@Test
	public void indicatesFailureXAException()
	{
		Assert.assertTrue(this.dialect.indicatesFailure(new XAException(XAException.XAER_RMFAIL)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_HEURCOM)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_HEURHAZ)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_HEURMIX)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_HEURRB)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_NOMIGRATE)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RBBASE)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RBCOMMFAIL)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RBDEADLOCK)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RBDEADLOCK)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RBEND)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RBINTEGRITY)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RBOTHER)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RBPROTO)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RBROLLBACK)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RBTIMEOUT)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RBTRANSIENT)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RDONLY)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RETRY)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XAER_ASYNC)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XAER_DUPID)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XAER_INVAL)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XAER_NOTA)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XAER_OUTSIDE)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XAER_PROTO)));
		Assert.assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XAER_RMERR)));
	}
}
