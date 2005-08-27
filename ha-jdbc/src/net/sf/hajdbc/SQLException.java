/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2004 Paul Ferraro
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
package net.sf.hajdbc;

/**
 * Wrapper for java.sql.SQLException that handles nested exceptions.
 * For convenience, this wrapper also initializes SQLException chains using the Java 1.4 nested exception model.
 * 
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class SQLException extends java.sql.SQLException
{
	private static final long serialVersionUID = 4122254034733510710L;

	/**
	 * Constructs a new SQLException.
	 * @param message the description of this exception
	 */
	public SQLException(String message)
	{
		super(message);
	}
	
	/**
	 * Constructs a new SQLException.
	 * @param cause the cause of this exception
	 */
	public SQLException(java.sql.SQLException cause)
	{
		super(cause.getMessage(), cause.getSQLState(), cause.getErrorCode());
		
		this.initSQLCause(cause);
	}
	
	/**
	 * Constructs a new SQLException.
	 * @param message
	 * @param cause
	 */
	public SQLException(String message, java.sql.SQLException cause)
	{
		super(message, cause.getSQLState(), cause.getErrorCode());
		
		this.initSQLCause(cause);
	}

	/**
	 * Constructs a new SQLException.
	 * @param message the description of this exception
	 * @param cause the cause of this exception
	 */
	public SQLException(String message, Throwable cause)
	{
		super(message);

		this.initCause(cause);
	}
	
	/**
	 * Constructs a new SQLException.
	 * @param cause the cause of this exception
	 */
	public SQLException(Throwable cause)
	{
		super(cause.getMessage());
		
		this.initCause(cause);
	}

	/**
	 * @see java.lang.Throwable#initCause(java.lang.Throwable)
	 */
	public synchronized Throwable initCause(Throwable cause)
	{
		if (java.sql.SQLException.class.isInstance(cause))
		{
			this.initSQLCause((java.sql.SQLException) cause);
		}
		
		return super.initCause(cause);
	}
	
	/**
	 * Initializes the SQLException chain using Java 1.4 nested exceptions.
	 * @param exception an exception in the chain
	 */
	private void initSQLCause(java.sql.SQLException exception)
	{
		java.sql.SQLException nextException = exception.getNextException();
		
		if (nextException != null)
		{
			if (exception.getCause() == null)
			{
				exception.initCause(nextException);
			}
			
			this.initSQLCause(nextException);
		}
	}
}
