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

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

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
		
		Assert.assertSame(message, result.getMessage());
		Assert.assertNull(result.getCause());
	}
	
	@Test
	public void createExceptionFromException()
	{
		Exception exception = new Exception("message");
		
		XAException result = this.factory.createException(exception);
		
		Assert.assertNull(result.getMessage());
		Assert.assertSame(exception, result.getCause());
	}
	
	@Test
	public void createExceptionFromXAException()
	{
		XAException exception = new XAException();
		
		XAException result = this.factory.createException(exception);
		
		Assert.assertSame(exception, result);
	}
	
	@Test
	public void getType()
	{
		ExceptionType result = this.factory.getType();
		
		Assert.assertSame(ExceptionType.XA, result);
	}
	
	@Test
	public void equals()
	{
		Assert.assertTrue(this.factory.equals(new XAException(), new XAException()));
		
		Assert.assertTrue(this.factory.equals(new XAException(XAException.XA_HEURCOM), new XAException(XAException.XA_HEURCOM)));
		Assert.assertFalse(this.factory.equals(new XAException(XAException.XA_HEURCOM), new XAException(XAException.XA_HEURHAZ)));
		
		Assert.assertTrue(this.factory.equals(new XAException("reason"), new XAException("reason")));
		Assert.assertFalse(this.factory.equals(new XAException("reason1"), new XAException("reason2")));
	}
	
	@Test
	public void correctHeuristic()
	{
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_RBBASE), Phase.PREPARE));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_RBCOMMFAIL), Phase.PREPARE));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_RBDEADLOCK), Phase.PREPARE));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_RBEND), Phase.PREPARE));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_RBINTEGRITY), Phase.PREPARE));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_RBOTHER), Phase.PREPARE));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_RBPROTO), Phase.PREPARE));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_RBROLLBACK), Phase.PREPARE));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_RBTIMEOUT), Phase.PREPARE));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_RBTRANSIENT), Phase.PREPARE));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_INVAL), Phase.PREPARE));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_NOTA), Phase.PREPARE));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_PROTO), Phase.PREPARE));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_RMERR), Phase.PREPARE));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_RMFAIL), Phase.PREPARE));
		
		Assert.assertTrue(this.factory.correctHeuristic(new XAException(XAException.XA_HEURCOM), Phase.COMMIT));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_HEURHAZ), Phase.COMMIT));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_HEURMIX), Phase.COMMIT));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_HEURRB), Phase.COMMIT));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_INVAL), Phase.COMMIT));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_NOTA), Phase.COMMIT));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_PROTO), Phase.COMMIT));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_RMERR), Phase.COMMIT));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_RMFAIL), Phase.COMMIT));
		
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_HEURCOM), Phase.ROLLBACK));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_HEURHAZ), Phase.ROLLBACK));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XA_HEURMIX), Phase.ROLLBACK));
		Assert.assertTrue(this.factory.correctHeuristic(new XAException(XAException.XA_HEURRB), Phase.ROLLBACK));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_INVAL), Phase.ROLLBACK));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_NOTA), Phase.ROLLBACK));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_PROTO), Phase.ROLLBACK));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_RMERR), Phase.ROLLBACK));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_RMFAIL), Phase.ROLLBACK));
		
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_INVAL), Phase.FORGET));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_NOTA), Phase.FORGET));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_PROTO), Phase.FORGET));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_RMERR), Phase.FORGET));
		Assert.assertFalse(this.factory.correctHeuristic(new XAException(XAException.XAER_RMFAIL), Phase.FORGET));
	}
	
	@Test
	public void indicatesFailure()
	{
		Dialect dialect = EasyMock.createStrictMock(Dialect.class);
		XAException exception = new XAException();
		
		EasyMock.expect(dialect.indicatesFailure(exception)).andReturn(true);
		
		EasyMock.replay(dialect);
		
		boolean result = this.factory.indicatesFailure(exception, dialect);
		
		EasyMock.verify(dialect);
		
		Assert.assertTrue(result);
	}
}
