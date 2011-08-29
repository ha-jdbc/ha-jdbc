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

import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.AdditionalMatchers.*;

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
		assertNull(this.dialect.getSequenceSupport());
	}
	
	@Test
	public void getIdentityColumnSupport()
	{
		assertNull(this.dialect.getIdentityColumnSupport());
	}
	
	@Test
	public void getAlterSequenceSQL() throws SQLException
	{
		SequenceSupport support = this.dialect.getSequenceSupport();
		
		if (support != null)
		{
			SequenceProperties sequence = mock(SequenceProperties.class);
			
			when(sequence.getName()).thenReturn("sequence");
			when(sequence.getIncrement()).thenReturn(1);
			
			String result = support.getAlterSequenceSQL(sequence, 1000L);
			
			assertEquals("ALTER SEQUENCE sequence RESTART WITH 1000", result);
		}
	}

	@Test
	public void getColumnType() throws SQLException
	{
		ColumnProperties column = mock(ColumnProperties.class);
		
		when(column.getType()).thenReturn(Types.INTEGER);
		
		int result = this.dialect.getColumnType(column);
		
		assertEquals(Types.INTEGER, result);
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
		
		assertEquals("ALTER TABLE table ADD CONSTRAINT name FOREIGN KEY (column1, column2) REFERENCES foreign_table (foreign_column1, foreign_column2) ON DELETE CASCADE ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED", result);
	}

	@Test
	public void getCreateUniqueConstraintSQL() throws SQLException
	{
		UniqueConstraint key = new UniqueConstraintImpl("name", "table");
		key.getColumnList().add("column1");
		key.getColumnList().add("column2");
		
		String result = this.dialect.getCreateUniqueConstraintSQL(key);
		
		assertEquals("ALTER TABLE table ADD CONSTRAINT name UNIQUE (column1, column2)", result);
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
		
		assertEquals("ALTER TABLE table DROP CONSTRAINT name", result);
	}

	@Test
	public void getDropUniqueConstraintSQL() throws SQLException
	{
		UniqueConstraint key = new UniqueConstraintImpl("name", "table");
		key.getColumnList().add("column1");
		key.getColumnList().add("column2");
		
		String result = this.dialect.getDropUniqueConstraintSQL(key);
		
		assertEquals("ALTER TABLE table DROP CONSTRAINT name", result);
	}

	@Test
	public void getNextSequenceValueSQL() throws SQLException
	{
		SequenceSupport support = this.dialect.getSequenceSupport();
		
		if (support != null)
		{
			SequenceProperties sequence = mock(SequenceProperties.class);
			
			when(sequence.getName()).thenReturn("sequence");
			
			String result = support.getNextSequenceValueSQL(sequence);
			
			assertEquals("SELECT NEXT VALUE FOR sequence", result);
		}
	}

	@Test
	public void getSequences() throws SQLException
	{
		SequenceSupport support = this.dialect.getSequenceSupport();
		
		if (support != null)
		{
			DatabaseMetaData metaData = mock(DatabaseMetaData.class);
			ResultSet resultSet = mock(ResultSet.class);
			
			when(metaData.getTables(eq(""), eq((String) null), eq("%"), aryEq(new String[] { "SEQUENCE" }))).thenReturn(resultSet);
			when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
			when(resultSet.getString("TABLE_SCHEM")).thenReturn("schema1").thenReturn("schema2");
			when(resultSet.getString("TABLE_NAME")).thenReturn("sequence1").thenReturn("sequence2");
			
			Map<QualifiedName, Integer> results = support.getSequences(metaData);
			
			verify(resultSet).close();
			
			assertEquals(2, results.size());
			
			Iterator<Map.Entry<QualifiedName, Integer>> iterator = results.entrySet().iterator();
			Map.Entry<QualifiedName, Integer> sequence = iterator.next();
	
			assertEquals("schema1", sequence.getKey().getSchema());
			assertEquals("sequence1", sequence.getKey().getName());
			assertEquals(1, sequence.getValue().intValue());
			
			sequence = iterator.next();
	
			assertEquals("schema2", sequence.getKey().getSchema());
			assertEquals("sequence2", sequence.getKey().getName());
			assertEquals(1, sequence.getValue().intValue());
		}
	}

	@Test
	public void getSimpleSQL() throws SQLException
	{
		String result = this.dialect.getSimpleSQL();

		assertEquals("SELECT CURRENT_TIMESTAMP", result);
	}

	@Test
	public void getTruncateTableSQL() throws SQLException
	{
		TableProperties table = mock(TableProperties.class);
		
		when(table.getName()).thenReturn("table");
		
		String result = this.dialect.getTruncateTableSQL(table);
		
		assertEquals("DELETE FROM table", result);
	}

	@Test
	public void isSelectForUpdate() throws SQLException
	{
		assertTrue(this.dialect.isSelectForUpdate("SELECT * FROM test FOR UPDATE"));
		assertFalse(this.dialect.isSelectForUpdate("SELECT * FROM test"));
	}

	@Test
	public void parseSequence() throws SQLException
	{
		SequenceSupport support = this.dialect.getSequenceSupport();
		
		if (support != null)
		{
			assertEquals("test", support.parseSequence("SELECT NEXT VALUE FOR test"));
			assertEquals("test", support.parseSequence("SELECT NEXT VALUE FOR test, * FROM table"));
			assertEquals("test", support.parseSequence("INSERT INTO table VALUES (NEXT VALUE FOR test)"));
			assertEquals("test", support.parseSequence("UPDATE table SET id = NEXT VALUE FOR test"));
			assertNull(support.parseSequence("SELECT * FROM table"));
		}
	}

	@Test
	public void getDefaultSchemas() throws SQLException
	{
		DatabaseMetaData metaData = mock(DatabaseMetaData.class);
		
		String user = "user";
		
		when(metaData.getUserName()).thenReturn(user);
		
		List<String> result = this.dialect.getDefaultSchemas(metaData);
		
		assertEquals(1, result.size());
		assertSame(user, result.get(0));
	}

	@Test
	public void parseInsertTable() throws SQLException
	{
		IdentityColumnSupport support = this.dialect.getIdentityColumnSupport();
		
		if (support != null)
		{
			assertEquals("table", support.parseInsertTable("INSERT INTO table (column1, column2) VALUES (1, 2)"));
			assertEquals("table", support.parseInsertTable("INSERT INTO table VALUES (1, 2)"));
			assertEquals("table", support.parseInsertTable("INSERT table (column1, column2) VALUES (1, 2)"));
			assertEquals("table", support.parseInsertTable("INSERT table VALUES (1, 2)"));
			assertEquals("table", support.parseInsertTable("INSERT INTO table (column1, column2) SELECT column1, column2 FROM dummy"));
			assertEquals("table", support.parseInsertTable("INSERT INTO table SELECT column1, column2 FROM dummy"));
			assertEquals("table", support.parseInsertTable("INSERT table (column1, column2) SELECT column1, column2 FROM dummy"));
			assertEquals("table", support.parseInsertTable("INSERT table SELECT column1, column2 FROM dummy"));
			assertNull(support.parseInsertTable("SELECT * FROM table WHERE 0=1"));
			assertNull(support.parseInsertTable("UPDATE table SET column = 0"));
		}
	}
	
	@Test
	public void getIdentifierPattern() throws SQLException
	{
		DatabaseMetaData metaData = mock(DatabaseMetaData.class);
		
		when(metaData.getExtraNameCharacters()).thenReturn("$");
		
		String result = this.dialect.getIdentifierPattern(metaData).pattern();
		
		assertEquals("[a-zA-Z][\\w\\Q$\\E]*", result);
	}

	@Test
	public void evaluateCurrentDate()
	{
		java.sql.Date date = new java.sql.Date(System.currentTimeMillis());
		
		assertEquals(String.format("SELECT DATE '%s' FROM test", date.toString()), this.dialect.evaluateCurrentDate("SELECT CURRENT_DATE FROM test", date));
		assertEquals("SELECT CCURRENT_DATE FROM test", this.dialect.evaluateCurrentDate("SELECT CCURRENT_DATE FROM test", date));
		assertEquals("SELECT CURRENT_DATES FROM test", this.dialect.evaluateCurrentDate("SELECT CURRENT_DATES FROM test", date));
		assertEquals("SELECT CURRENT_TIME FROM test", this.dialect.evaluateCurrentDate("SELECT CURRENT_TIME FROM test", date));
		assertEquals("SELECT CURRENT_TIMESTAMP FROM test", this.dialect.evaluateCurrentDate("SELECT CURRENT_TIMESTAMP FROM test", date));
	}

	@Test
	public void evaluateCurrentTime()
	{
		java.sql.Time time = new java.sql.Time(System.currentTimeMillis());
		
		assertEquals(String.format("SELECT TIME '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT CURRENT_TIME FROM test", time));
		assertEquals(String.format("SELECT TIME '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT CURRENT_TIME(2) FROM test", time));
		assertEquals(String.format("SELECT TIME '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT CURRENT_TIME ( 2 ) FROM test", time));
		assertEquals(String.format("SELECT TIME '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT LOCALTIME FROM test", time));
		assertEquals(String.format("SELECT TIME '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT LOCALTIME(2) FROM test", time));
		assertEquals(String.format("SELECT TIME '%s' FROM test", time.toString()), this.dialect.evaluateCurrentTime("SELECT LOCALTIME ( 2 ) FROM test", time));
		assertEquals("SELECT CCURRENT_TIME FROM test", this.dialect.evaluateCurrentTime("SELECT CCURRENT_TIME FROM test", time));
		assertEquals("SELECT LLOCALTIME FROM test", this.dialect.evaluateCurrentTime("SELECT LLOCALTIME FROM test", time));
		assertEquals("SELECT CURRENT_DATE FROM test", this.dialect.evaluateCurrentTime("SELECT CURRENT_DATE FROM test", time));
		assertEquals("SELECT CURRENT_TIMESTAMP FROM test", this.dialect.evaluateCurrentTime("SELECT CURRENT_TIMESTAMP FROM test", time));
		assertEquals("SELECT LOCALTIMESTAMP FROM test", this.dialect.evaluateCurrentTime("SELECT LOCALTIMESTAMP FROM test", time));
	}

	@Test
	public void evaluateCurrentTimestamp()
	{
		java.sql.Timestamp timestamp = new java.sql.Timestamp(System.currentTimeMillis());
		
		assertEquals(String.format("SELECT TIMESTAMP '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_TIMESTAMP FROM test", timestamp));
		assertEquals(String.format("SELECT TIMESTAMP '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_TIMESTAMP(2) FROM test", timestamp));
		assertEquals(String.format("SELECT TIMESTAMP '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_TIMESTAMP ( 2 ) FROM test", timestamp));
		assertEquals(String.format("SELECT TIMESTAMP '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT LOCALTIMESTAMP FROM test", timestamp));
		assertEquals(String.format("SELECT TIMESTAMP '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT LOCALTIMESTAMP(2) FROM test", timestamp));
		assertEquals(String.format("SELECT TIMESTAMP '%s' FROM test", timestamp.toString()), this.dialect.evaluateCurrentTimestamp("SELECT LOCALTIMESTAMP ( 2 ) FROM test", timestamp));
		assertEquals("SELECT CCURRENT_TIMESTAMP FROM test", this.dialect.evaluateCurrentTimestamp("SELECT CCURRENT_TIMESTAMP FROM test", timestamp));
		assertEquals("SELECT LLOCALTIMESTAMP FROM test", this.dialect.evaluateCurrentTimestamp("SELECT LLOCALTIMESTAMP FROM test", timestamp));
		assertEquals("SELECT CURRENT_DATE FROM test", this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_DATE FROM test", timestamp));
		assertEquals("SELECT CURRENT_TIME FROM test", this.dialect.evaluateCurrentTimestamp("SELECT CURRENT_TIME FROM test", timestamp));
		assertEquals("SELECT LOCALTIME FROM test", this.dialect.evaluateCurrentTimestamp("SELECT LOCALTIME FROM test", timestamp));
	}

	@Test
	public void evaluateRand()
	{
		assertTrue(Pattern.matches("SELECT ((0\\.\\d+)|([1-9]\\.\\d+E\\-\\d+)) FROM test", this.dialect.evaluateRand("SELECT RAND() FROM test")));
		assertTrue(Pattern.matches("SELECT ((0\\.\\d+)|([1-9]\\.\\d+E\\-\\d+)) FROM test", this.dialect.evaluateRand("SELECT RAND ( ) FROM test")));
		assertEquals("SELECT RAND FROM test", this.dialect.evaluateRand("SELECT RAND FROM test"));
		assertEquals("SELECT OPERAND() FROM test", this.dialect.evaluateRand("SELECT OPERAND() FROM test"));
		assertEquals("SELECT 1 FROM test", this.dialect.evaluateRand("SELECT 1 FROM test"));
	}

	@Test
	public void getAlterIdentityColumnSQL() throws SQLException
	{
		IdentityColumnSupport support = this.dialect.getIdentityColumnSupport();
		
		if (support != null)
		{
			TableProperties table = mock(TableProperties.class);
			ColumnProperties column = mock(ColumnProperties.class);
			
			when(table.getName()).thenReturn("table");
			when(column.getName()).thenReturn("column");
			
			String result = support.getAlterIdentityColumnSQL(table, column, 1000L);
			
			assertEquals("ALTER TABLE table ALTER COLUMN column RESTART WITH 1000", result);
		}
	}
	
	@Test
	public void indicatesFailureSQLException()
	{
		assertTrue(this.dialect.indicatesFailure(new SQLNonTransientConnectionException()));
		assertFalse(this.dialect.indicatesFailure(new SQLException()));
		assertFalse(this.dialect.indicatesFailure(new BatchUpdateException()));
		assertFalse(this.dialect.indicatesFailure(new RowSetWarning()));
		assertFalse(this.dialect.indicatesFailure(new SerialException()));
		assertFalse(this.dialect.indicatesFailure(new SQLClientInfoException()));
		assertFalse(this.dialect.indicatesFailure(new SQLNonTransientException()));
		assertFalse(this.dialect.indicatesFailure(new SQLDataException()));
		assertFalse(this.dialect.indicatesFailure(new SQLFeatureNotSupportedException()));
		assertFalse(this.dialect.indicatesFailure(new SQLIntegrityConstraintViolationException()));
		assertFalse(this.dialect.indicatesFailure(new SQLInvalidAuthorizationSpecException()));
		assertFalse(this.dialect.indicatesFailure(new SQLSyntaxErrorException()));
		assertFalse(this.dialect.indicatesFailure(new SQLRecoverableException()));
		assertFalse(this.dialect.indicatesFailure(new SQLTransientException()));
		assertFalse(this.dialect.indicatesFailure(new SQLTimeoutException()));
		assertFalse(this.dialect.indicatesFailure(new SQLTransactionRollbackException()));
		assertFalse(this.dialect.indicatesFailure(new SQLTransientConnectionException()));
		assertFalse(this.dialect.indicatesFailure(new SQLWarning()));
		assertFalse(this.dialect.indicatesFailure(new DataTruncation(1, false, false, 1, 1)));
		assertFalse(this.dialect.indicatesFailure(new SQLDataException()));
		assertFalse(this.dialect.indicatesFailure(new SyncFactoryException()));
		assertFalse(this.dialect.indicatesFailure(new SyncProviderException()));
	}
	
	@Test
	public void indicatesFailureXAException()
	{
		assertTrue(this.dialect.indicatesFailure(new XAException(XAException.XAER_RMFAIL)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_HEURCOM)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_HEURHAZ)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_HEURMIX)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_HEURRB)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_NOMIGRATE)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RBBASE)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RBCOMMFAIL)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RBDEADLOCK)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RBDEADLOCK)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RBEND)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RBINTEGRITY)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RBOTHER)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RBPROTO)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RBROLLBACK)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RBTIMEOUT)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RBTRANSIENT)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RDONLY)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XA_RETRY)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XAER_ASYNC)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XAER_DUPID)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XAER_INVAL)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XAER_NOTA)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XAER_OUTSIDE)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XAER_PROTO)));
		assertFalse(this.dialect.indicatesFailure(new XAException(XAException.XAER_RMERR)));
	}
}
