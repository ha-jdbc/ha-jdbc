/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
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
import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.durability.Durability.Phase;
import net.sf.hajdbc.sql.AbstractExceptionFactory;

/**
 * @author Paul Ferraro
 */
public class XAExceptionFactory extends AbstractExceptionFactory<XAException>
{
	public XAExceptionFactory()
	{
		super(XAException.class);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.ExceptionFactory#createException(java.lang.String)
	 */
	@Override
	public XAException createException(String message)
	{
		return new XAException(message);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.AbstractExceptionFactory#createException()
	 */
	@Override
	protected XAException createException()
	{
		return new XAException();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.ExceptionFactory#equals(java.lang.Exception, java.lang.Exception)
	 */
	@Override
	public boolean equals(XAException exception1, XAException exception2)
	{
		// Match by error code, if defined
		if ((exception1.errorCode != 0) || (exception2.errorCode != 0))
		{
			return exception1.errorCode == exception2.errorCode;
		}

		// Fallback to match by message
		String message1 = exception1.getMessage();
		String message2 = exception2.getMessage();

		return (message1 == message2) || ((message1 != null) && (message2 != null) && message1.equals(message2));
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.ExceptionFactory#indicatesFailure(java.lang.Exception, net.sf.hajdbc.Dialect)
	 */
	@Override
	public boolean indicatesFailure(XAException exception, Dialect dialect)
	{
		return dialect.indicatesFailure(exception);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.ExceptionFactory#getType()
	 */
	@Override
	public ExceptionType getType()
	{
		return ExceptionType.XA;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.ExceptionFactory#correctHeuristic(java.lang.Exception, net.sf.hajdbc.durability.Durability.Phase)
	 */
	@Override
	public boolean correctHeuristic(XAException exception, Phase phase)
	{
		switch (phase)
		{
			case COMMIT:
			{
				return exception.errorCode == XAException.XA_HEURCOM;
			}
			case ROLLBACK:
			{
				return exception.errorCode == XAException.XA_HEURRB;
			}
			default:
			{
				return false;
			}
		}
	}
}
