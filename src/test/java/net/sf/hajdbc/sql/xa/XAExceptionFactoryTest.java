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
package net.sf.hajdbc.sql.xa;

import javax.transaction.xa.XAException;

import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ExceptionFactory;
import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.durability.Durability.Phase;

import static org.junit.Assert.*;
import org.junit.Test;
import static org.mockito.Mockito.*;

/**
 * @author Paul Ferraro
 */
public class XAExceptionFactoryTest
{
	private final ExceptionFactory<XAException> factory = new XAExceptionFactory();
	
	@Test
	public void createExceptionFromMessage()
	{
		String message = "message";
		
		XAException result = this.factory.createException(message);
		
		assertSame(message, result.getMessage());
		assertNull(result.getCause());
	}
	
	@Test
	public void createExceptionFromException()
	{
		Exception exception = new Exception("message");
		
		XAException result = this.factory.createException(exception);
		
		assertNotNull(result.getMessage());
		assertEquals(exception.getMessage(), result.getMessage());
		assertSame(exception, result.getCause());
	}
	
	@Test
	public void createExceptionFromXAException()
	{
		XAException exception = new XAException();
		
		XAException result = this.factory.createException(exception);
		
		assertSame(exception, result);
	}
	
	@Test
	public void getType()
	{
		ExceptionType result = this.factory.getType();
		
		assertSame(ExceptionType.XA, result);
	}
	
	@Test
	public void equals()
	{
		assertTrue(this.factory.equals(new XAException(), new XAException()));
		
		assertTrue(this.factory.equals(new XAException(XAException.XA_HEURCOM), new XAException(XAException.XA_HEURCOM)));
		assertFalse(this.factory.equals(new XAException(XAException.XA_HEURCOM), new XAException(XAException.XA_HEURHAZ)));
		
		assertTrue(this.factory.equals(new XAException("reason"), new XAException("reason")));
		assertFalse(this.factory.equals(new XAException("reason1"), new XAException("reason2")));
	}
	
	@Test
	public void correctHeuristic()
	{
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_RBBASE), Phase.PREPARE));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_RBCOMMFAIL), Phase.PREPARE));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_RBDEADLOCK), Phase.PREPARE));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_RBEND), Phase.PREPARE));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_RBINTEGRITY), Phase.PREPARE));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_RBOTHER), Phase.PREPARE));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_RBPROTO), Phase.PREPARE));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_RBROLLBACK), Phase.PREPARE));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_RBTIMEOUT), Phase.PREPARE));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_RBTRANSIENT), Phase.PREPARE));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_INVAL), Phase.PREPARE));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_NOTA), Phase.PREPARE));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_PROTO), Phase.PREPARE));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_RMERR), Phase.PREPARE));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_RMFAIL), Phase.PREPARE));
		
		assertTrue(this.factory.correctHeuristic(new XAException(XAException.XA_HEURCOM), Phase.COMMIT));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_HEURHAZ), Phase.COMMIT));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_HEURMIX), Phase.COMMIT));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_HEURRB), Phase.COMMIT));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_INVAL), Phase.COMMIT));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_NOTA), Phase.COMMIT));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_PROTO), Phase.COMMIT));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_RMERR), Phase.COMMIT));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_RMFAIL), Phase.COMMIT));
		
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_HEURCOM), Phase.ROLLBACK));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_HEURHAZ), Phase.ROLLBACK));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_HEURMIX), Phase.ROLLBACK));
		assertTrue(this.factory.correctHeuristic(new XAException(XAException.XA_HEURRB), Phase.ROLLBACK));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_INVAL), Phase.ROLLBACK));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_NOTA), Phase.ROLLBACK));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_PROTO), Phase.ROLLBACK));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_RMERR), Phase.ROLLBACK));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_RMFAIL), Phase.ROLLBACK));
		
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_INVAL), Phase.FORGET));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_NOTA), Phase.FORGET));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_PROTO), Phase.FORGET));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_RMERR), Phase.FORGET));
		assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_RMFAIL), Phase.FORGET));
	}
	
	@Test
	public void indicatesFailure()
	{
		Dialect dialect = mock(Dialect.class);
		XAException exception = new XAException();
		
		when(dialect.indicatesFailure(exception)).thenReturn(true);
		
		boolean result = this.factory.indicatesFailure(exception, dialect);
		
		assertTrue(result);
	}
}
