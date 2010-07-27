/*
 * HA-JDBC: High-Availablity JDBC
 * Copyright 2010 Paul Ferraro
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
package net.sf.hajdbc.sql;

import java.sql.ClientInfoStatus;
import java.sql.SQLException;
import java.util.Collections;

import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.durability.Durability.Phase;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Paul Ferraro
 */
public class SQLExceptionFactoryTest
{
	private final ExceptionFactory<SQLException> factory = new SQLExceptionFactory();
	
	@Test
	public void createExceptionFromMessage()
	{
		String message = "message";
		
		SQLException result = this.factory.createException(message);
		
		Assert.assertSame(message, result.getMessage());
		Assert.assertNull(result.getCause());
	}
	
	@Test
	public void createExceptionFromException()
	{
		Exception exception = new Exception("message");
		
		SQLException result = this.factory.createException(exception);
		
		Assert.assertNull(result.getMessage());
		Assert.assertSame(exception, result.getCause());
	}
	
	@Test
	public void createExceptionFromSQLException()
	{
		SQLException exception = new SQLException();
		
		SQLException result = this.factory.createException(exception);
		
		Assert.assertSame(exception, result);
	}
	
	@Test
	public void getType()
	{
		ExceptionType result = this.factory.getType();
		
		Assert.assertSame(ExceptionType.SQL, result);
	}
	
	@Test
	public void equals()
	{
		Assert.assertTrue(this.factory.equals(null, null));
		Assert.assertFalse(this.factory.equals(new SQLException(), null));
		Assert.assertTrue(this.factory.equals(new SQLException(), new SQLException()));

		Assert.assertTrue(this.factory.equals(new java.sql.SQLDataException(), new java.sql.SQLDataException()));
		Assert.assertFalse(this.factory.equals(new java.sql.SQLClientInfoException(), new java.sql.SQLDataException()));

		Assert.assertTrue(this.factory.equals(new java.sql.BatchUpdateException(new int[] { 1, 2 }), new java.sql.BatchUpdateException(new int[] { 1, 2 })));
		Assert.assertFalse(this.factory.equals(new java.sql.BatchUpdateException(new int[] { 1, 2 }), new java.sql.BatchUpdateException(new int[] { 1, 3 })));

		Assert.assertTrue(this.factory.equals(new java.sql.DataTruncation(1, true, true, 1, 1), new java.sql.DataTruncation(1, true, true, 1, 1)));
		Assert.assertFalse(this.factory.equals(new java.sql.DataTruncation(1, true, true, 1, 1), new java.sql.DataTruncation(2, true, true, 1, 1)));
		Assert.assertFalse(this.factory.equals(new java.sql.DataTruncation(1, true, true, 1, 1), new java.sql.DataTruncation(1, false, true, 1, 1)));
		Assert.assertFalse(this.factory.equals(new java.sql.DataTruncation(1, true, true, 1, 1), new java.sql.DataTruncation(1, true, false, 1, 1)));
		Assert.assertFalse(this.factory.equals(new java.sql.DataTruncation(1, true, true, 1, 1), new java.sql.DataTruncation(1, true, true, 2, 1)));
		Assert.assertFalse(this.factory.equals(new java.sql.DataTruncation(1, true, true, 1, 1), new java.sql.DataTruncation(1, true, true, 1, 2)));
		
		Assert.assertTrue(this.factory.equals(new java.sql.SQLClientInfoException(Collections.singletonMap("test", ClientInfoStatus.REASON_UNKNOWN)), new java.sql.SQLClientInfoException(Collections.singletonMap("test", ClientInfoStatus.REASON_UNKNOWN))));
		Assert.assertFalse(this.factory.equals(new java.sql.SQLClientInfoException(Collections.singletonMap("test", ClientInfoStatus.REASON_UNKNOWN)), new java.sql.SQLClientInfoException(Collections.singletonMap("test", ClientInfoStatus.REASON_UNKNOWN_PROPERTY))));
		
		Assert.assertTrue(this.factory.equals(new SQLException("reason1", "sql.state1", 1), new SQLException("reason2", "sql.state2", 1)));
		Assert.assertFalse(this.factory.equals(new SQLException("reason", "sql.state", 1), new SQLException("reason", "sql.state", 2)));
		Assert.assertFalse(this.factory.equals(new SQLException("reason", "sql.state", 1), new SQLException("reason", "sql.state", 0)));
		
		Assert.assertTrue(this.factory.equals(new SQLException("reason1", "sql.state", 0), new SQLException("reason2", "sql.state", 0)));
		Assert.assertFalse(this.factory.equals(new SQLException("reason", "sql.state1", 0), new SQLException("reason", "sql.state2", 0)));
		Assert.assertFalse(this.factory.equals(new SQLException("reason", "sql.state1", 0), new SQLException("reason", null, 0)));
		
		Assert.assertTrue(this.factory.equals(new SQLException("reason", null, 0), new SQLException("reason", null, 0)));
		Assert.assertFalse(this.factory.equals(new SQLException("reason1", null, 0), new SQLException("reason2", null, 0)));
		
		SQLException exception1 = new SQLException();
		SQLException exception2 = new SQLException();
		
		exception1.setNextException(new SQLException());
		exception2.setNextException(new SQLException());
		
		Assert.assertTrue(this.factory.equals(exception1, exception2));
		
		exception1.setNextException(new SQLException("reason1"));
		exception2.setNextException(new SQLException("reason2"));
		
		Assert.assertFalse(this.factory.equals(exception1, exception2));
	}
	
	@Test
	public void correctHeuristic()
	{
		Assert.assertFalse(this.factory.correctHeuristic(new SQLException(), Phase.COMMIT));
		Assert.assertFalse(this.factory.correctHeuristic(new SQLException(), Phase.FORGET));
		Assert.assertFalse(this.factory.correctHeuristic(new SQLException(), Phase.PREPARE));
		Assert.assertFalse(this.factory.correctHeuristic(new SQLException(), Phase.ROLLBACK));
	}
	
	@Test
	public void indicatesFailure()
	{
		Dialect dialect = EasyMock.createStrictMock(Dialect.class);
		SQLException exception = new SQLException();
		SQLException nextException = new SQLException();
		
		EasyMock.expect(dialect.indicatesFailure(exception)).andReturn(true);
		
		EasyMock.replay(dialect);
		
		boolean result = this.factory.indicatesFailure(exception, dialect);
		
		EasyMock.verify(dialect);
		
		Assert.assertTrue(result);
		
		EasyMock.reset(dialect);
		
		exception.setNextException(nextException);
		
		EasyMock.expect(dialect.indicatesFailure(exception)).andReturn(false);
		EasyMock.expect(dialect.indicatesFailure(nextException)).andReturn(true);
		
		EasyMock.replay(dialect);
		
		result = this.factory.indicatesFailure(exception, dialect);
		
		EasyMock.verify(dialect);
		
		Assert.assertTrue(result);
		
		EasyMock.reset(dialect);
	}
}
