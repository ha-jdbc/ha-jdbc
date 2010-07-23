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
package net.sf.hajdbc.sql;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.util.Arrays;

import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.durability.Durability.Phase;

/**
 * @author paul
 *
 */
public class SQLExceptionFactory extends AbstractExceptionFactory<SQLException>
{
/*
	public static ExceptionFactory<SQLException> getInstance()
	{
		return ExceptionType.SQL.getExceptionFactory();
	}
*/	
	public SQLExceptionFactory()
	{
		super(SQLException.class);	
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.ExceptionFactory#createException(java.lang.String)
	 */
	@Override
	public SQLException createException(String message)
	{
		return new SQLException(message);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.sql.AbstractExceptionFactory#createException()
	 */
	@Override
	protected SQLException createException()
	{
		return new SQLException();
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.ExceptionFactory#equals(java.lang.Exception, java.lang.Exception)
	 */
	@Override
	public boolean equals(SQLException exception1, SQLException exception2)
	{
		// Terminator for exception chain recursion
		if ((exception1 == null) || (exception2 == null))
		{
			return exception1 == exception2;
		}
		
		// Fast-fail for mismatched Java 1.6 SQLException subclasses
		if (!exception1.getClass().equals(exception2.getClass()))
		{
			return false;
		}
		
		// Ensure BatchUpdateExceptions have matching update counts
		if ((exception1 instanceof BatchUpdateException) && (exception2 instanceof BatchUpdateException))
		{
			BatchUpdateException e1 = (BatchUpdateException) exception1;
			BatchUpdateException e2 = (BatchUpdateException) exception2;
			
			int[] counts1 = e1.getUpdateCounts();
			int[] counts2 = e2.getUpdateCounts();
			
			if ((counts1 != null) && (counts2 != null) ? !Arrays.equals(counts1, counts2) : (counts1 != counts2))
			{
				return false;
			}
		}
		
		SQLException nextException1 = exception1.getNextException();
		SQLException nextException2 = exception2.getNextException();
		
		int code1 = exception1.getErrorCode();
		int code2 = exception2.getErrorCode();

		// Match by vendor code, if defined
		if ((code1 != 0) || (code2 != 0))
		{
			return (code1 == code2) ? this.equals(nextException1, nextException2) : false;
		}
		
		String state1 = exception1.getSQLState();
		String state2 = exception2.getSQLState();

		boolean hasState1 = (state1 != null);
		boolean hasState2 = (state2 != null);

		// Match by SQLState, if defined
		if (hasState1 || hasState2)
		{
			return (state1 == state2) || (hasState1 && hasState2 && state1.equals(state2)) ? this.equals(nextException1, nextException2) : false;
		}

		// Fallback to match by reason
		String reason1 = exception1.getMessage();
		String reason2 = exception2.getMessage();
		
		return ((reason1 == reason2) || ((reason1 != null) && (reason2 != null) && reason1.equals(reason2))) ? this.equals(nextException1, nextException2) : false;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.ExceptionFactory#indicatesFailure(java.lang.Exception, net.sf.hajdbc.Dialect)
	 */
	@Override
	public boolean indicatesFailure(SQLException exception, Dialect dialect)
	{
		SQLException nextException = exception.getNextException();
		
		return dialect.indicatesFailure(exception) || ((nextException != null) && this.indicatesFailure(nextException, dialect));
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.ExceptionFactory#getType()
	 */
	@Override
	public ExceptionType getType()
	{
		return ExceptionType.SQL;
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.ExceptionFactory#correctHeuristic(java.lang.Exception, net.sf.hajdbc.durability.Durability.Phase)
	 */
	@Override
	public boolean correctHeuristic(SQLException exception, Phase phase)
	{
		return false;
	}
}
