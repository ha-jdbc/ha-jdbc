/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.dialect;

import static org.junit.Assert.*;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.BatchUpdateException;
import java.sql.Connection;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import javax.sql.rowset.RowSetWarning;
import javax.sql.rowset.serial.SerialException;
import javax.sql.rowset.spi.SyncFactoryException;
import javax.sql.rowset.spi.SyncProviderException;
import javax.transaction.xa.XAException;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.Database;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.IdentityColumnSupport;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.SequencePropertiesFactory;
import net.sf.hajdbc.SequenceSupport;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.UniqueConstraint;
import net.sf.hajdbc.codec.Decoder;

import org.junit.Test;

/**
 * @author Paul Ferraro
 *
 */
public class StandardDialectTest
{
	private DialectFactory factory;
	Dialect dialect;
	
	public StandardDialectTest()
	{
		this(new StandardDialectFactory());
	}
	
	protected StandardDialectTest(DialectFactory factory)
	{
		this.factory = factory;
		this.dialect = factory.createDialect();
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
			QualifiedName name = mock(QualifiedName.class);
			
			when(sequence.getName()).thenReturn(name);
			when(name.getDDLName()).thenReturn("sequence");
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
		QualifiedName table = mock(QualifiedName.class);
		QualifiedName foreignTable = mock(QualifiedName.class);
		ForeignKeyConstraint constraint = mock(ForeignKeyConstraint.class);
		
		when(table.getDDLName()).thenReturn("table");
		when(foreignTable.getDDLName()).thenReturn("foreign_table");
		when(constraint.getName()).thenReturn("name");
		when(constraint.getTable()).thenReturn(table);
		when(constraint.getColumnList()).thenReturn(Arrays.asList("column1", "column2"));
		when(constraint.getForeignTable()).thenReturn(foreignTable);
		when(constraint.getForeignColumnList()).thenReturn(Arrays.asList("foreign_column1", "foreign_column2"));
		when(constraint.getDeferrability()).thenReturn(DatabaseMetaData.importedKeyInitiallyDeferred);
		when(constraint.getDeleteRule()).thenReturn(DatabaseMetaData.importedKeyCascade);
		when(constraint.getUpdateRule()).thenReturn(DatabaseMetaData.importedKeyRestrict);
		
		String result = this.dialect.getCreateForeignKeyConstraintSQL(constraint);
		
		assertEquals("ALTER TABLE table ADD CONSTRAINT name FOREIGN KEY (column1, column2) REFERENCES foreign_table (foreign_column1, foreign_column2) ON DELETE CASCADE ON UPDATE RESTRICT DEFERRABLE INITIALLY DEFERRED", result);
	}

	@Test
	public void getCreateUniqueConstraintSQL() throws SQLException
	{
		QualifiedName table = mock(QualifiedName.class);
		UniqueConstraint constraint = mock(UniqueConstraint.class);
		
		when(table.getDDLName()).thenReturn("table");
		when(constraint.getName()).thenReturn("name");
		when(constraint.getTable()).thenReturn(table);
		when(constraint.getColumnList()).thenReturn(Arrays.asList("column1", "column2"));
		
		String result = this.dialect.getCreateUniqueConstraintSQL(constraint);
		
		assertEquals("ALTER TABLE table ADD CONSTRAINT name UNIQUE (column1, column2)", result);
	}

	@Test
	public void getDropForeignKeyConstraintSQL() throws SQLException
	{
		QualifiedName table = mock(QualifiedName.class);
		QualifiedName foreignTable = mock(QualifiedName.class);
		ForeignKeyConstraint constraint = mock(ForeignKeyConstraint.class);
		
		when(table.getDDLName()).thenReturn("table");
		when(foreignTable.getDDLName()).thenReturn("foreign_table");
		when(constraint.getName()).thenReturn("name");
		when(constraint.getTable()).thenReturn(table);
		when(constraint.getColumnList()).thenReturn(Arrays.asList("column1", "column2"));
		when(constraint.getForeignTable()).thenReturn(foreignTable);
		when(constraint.getForeignColumnList()).thenReturn(Arrays.asList("foreign_column1", "foreign_column2"));
		when(constraint.getDeferrability()).thenReturn(DatabaseMetaData.importedKeyInitiallyDeferred);
		when(constraint.getDeleteRule()).thenReturn(DatabaseMetaData.importedKeyCascade);
		when(constraint.getUpdateRule()).thenReturn(DatabaseMetaData.importedKeyRestrict);
		
		String result = this.dialect.getDropForeignKeyConstraintSQL(constraint);
		
		assertEquals("ALTER TABLE table DROP CONSTRAINT name", result);
	}

	@Test
	public void getDropUniqueConstraintSQL() throws SQLException
	{
		QualifiedName table = mock(QualifiedName.class);
		UniqueConstraint constraint = mock(UniqueConstraint.class);
		
		when(table.getDDLName()).thenReturn("table");
		when(constraint.getName()).thenReturn("name");
		when(constraint.getTable()).thenReturn(table);
		when(constraint.getColumnList()).thenReturn(Arrays.asList("column1", "column2"));
		
		String result = this.dialect.getDropUniqueConstraintSQL(constraint);
		
		assertEquals("ALTER TABLE table DROP CONSTRAINT name", result);
	}

	@Test
	public void getNextSequenceValueSQL() throws SQLException
	{
		SequenceSupport support = this.dialect.getSequenceSupport();
		
		if (support != null)
		{
			QualifiedName name = mock(QualifiedName.class);
			SequenceProperties sequence = mock(SequenceProperties.class);
			
			when(sequence.getName()).thenReturn(name);
			when(name.getDMLName()).thenReturn("sequence");
			
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
			SequencePropertiesFactory factory = mock(SequencePropertiesFactory.class);
			SequenceProperties sequence1 = mock(SequenceProperties.class);
			SequenceProperties sequence2 = mock(SequenceProperties.class);
			DatabaseMetaData metaData = mock(DatabaseMetaData.class);
			ResultSet resultSet = mock(ResultSet.class);
			
			when(metaData.supportsSchemasInTableDefinitions()).thenReturn(true);
			when(metaData.supportsSchemasInDataManipulation()).thenReturn(true);
			when(metaData.getTables(eq(""), eq((String) null), eq("%"), aryEq(new String[] { "SEQUENCE" }))).thenReturn(resultSet);
			when(resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
			when(resultSet.getString("TABLE_SCHEM")).thenReturn("schema1").thenReturn("schema2");
			when(resultSet.getString("TABLE_NAME")).thenReturn("sequence1").thenReturn("sequence2");
			when(factory.createSequenceProperties("schema1", "sequence1", 1)).thenReturn(sequence1);
			when(factory.createSequenceProperties("schema2", "sequence2", 1)).thenReturn(sequence2);
			
			Collection<SequenceProperties> results = support.getSequences(metaData, factory);
			
			verify(resultSet).close();
			
			assertEquals(2, results.size());
			
			Iterator<SequenceProperties> sequences = results.iterator();

			assertSame(sequence1, sequences.next());
			assertSame(sequence2, sequences.next());
		}
	}

	@Test
	public void getTruncateTableSQL() throws SQLException
	{
		TableProperties table = mock(TableProperties.class);
		QualifiedName name = mock(QualifiedName.class);
		
		when(table.getName()).thenReturn(name);
		when(name.getDMLName()).thenReturn("table");
		
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
			QualifiedName name = mock(QualifiedName.class);
			
			when(table.getName()).thenReturn(name);
			when(name.getDDLName()).thenReturn("table");
			when(column.getName()).thenReturn("column");
			
			String result = support.getAlterIdentityColumnSQL(table, column, 1000L);
			
			assertEquals("ALTER TABLE table ALTER COLUMN column RESTART WITH 1000", result);
		}
	}
	
	@Test
	public void indicatesFailureSQLException()
	{
		assertTrue(this.dialect.indicatesFailure(new SQLNonTransientConnectionException()));
		assertTrue(this.dialect.indicatesFailure(new SQLTransientConnectionException()));
		int i = 0;
		char[] alphabet = new char[36];
		for (char c = '0'; c <= '9'; ++c)
		{
			alphabet[i++] = c;
		}
		for (char c = 'A'; c <= 'Z'; ++c)
		{
			alphabet[i++] = c;
		}
		for (int a = 0; a < alphabet.length; ++a)
		{
			for (int b = 0; b < alphabet.length; ++b)
			{
				this.indicatesFailure(String.format("%s%s000", alphabet[a], alphabet[b]));
			}
		}
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
		assertFalse(this.dialect.indicatesFailure(new SQLWarning()));
		assertFalse(this.dialect.indicatesFailure(new DataTruncation(1, false, false, 1, 1)));
		assertFalse(this.dialect.indicatesFailure(new SQLDataException()));
		assertFalse(this.dialect.indicatesFailure(new SyncFactoryException()));
		assertFalse(this.dialect.indicatesFailure(new SyncProviderException()));
	}
	
	protected void indicatesFailure(String sqlState)
	{
		SQLException exception = new SQLException("reason", String.valueOf(sqlState));
		if (sqlState.startsWith("08"))
		{
			assertTrue(sqlState, this.dialect.indicatesFailure(exception));
		}
		else
		{
			assertFalse(sqlState, this.dialect.indicatesFailure(exception));
		}
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
	
	@Test
	public void isValid() throws SQLException
	{
		Connection connection = mock(Connection.class);
		
		when(connection.isValid(0)).thenReturn(true);
		
		boolean result = this.dialect.isValid(connection);
		
		assertTrue(result);
	}

	@Test
	public void getConnectionProperties() throws SQLException
	{
		Database<Void> database = mock(Database.class);
		Decoder decoder = mock(Decoder.class);
		Connection connection = mock(Connection.class);
		DatabaseMetaData metaData = mock(DatabaseMetaData.class);
		String host = "myhost";
		String port = "1234";
		String databaseName = "mydb";
		String user = "user";
		String password = "password";
		
		when(database.decodePassword(decoder)).thenReturn(password);
		when(database.connect(null, password)).thenReturn(connection);
		when(connection.getMetaData()).thenReturn(metaData);
		when(metaData.getURL()).thenReturn(String.format("jdbc:%s://%s:%s/%s?loginTimeout=0&socketTimeout=0&prepareThreshold=5&unknownLength=2147483647&tcpKeepAlive=false&binaryTransfer=true", this.factory.getId(), host, port, databaseName));
		when(metaData.getUserName()).thenReturn(user);
		
		ConnectionProperties properties = this.dialect.getConnectionProperties(database, decoder);
		
		assertEquals(host, properties.getHost());
		assertEquals(port, properties.getPort());
		assertEquals(databaseName, properties.getDatabase());
		assertSame(user, properties.getUser());
		assertSame(password, properties.getPassword());
	}
}
