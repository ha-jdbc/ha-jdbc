/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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
	 * @see java.sql.SQLException#getErrorCode()
	 */
	public int getErrorCode()
	{
		Throwable cause = this.getCause();

		if ((cause != null) && java.sql.SQLException.class.isInstance(cause))
		{
			java.sql.SQLException exception = (java.sql.SQLException) cause;
			
			return exception.getErrorCode();
		}
		
		return super.getErrorCode();
	}

	/**
	 * @see java.sql.SQLException#getNextException()
	 */
	public java.sql.SQLException getNextException()
	{
		Throwable cause = this.getCause();
		
		if ((cause != null) && java.sql.SQLException.class.isInstance(cause))
		{
			java.sql.SQLException exception = (java.sql.SQLException) cause;
			
			return exception.getNextException();
		}
		
		return super.getNextException();
	}

	/**
	 * @see java.sql.SQLException#getSQLState()
	 */
	public String getSQLState()
	{
		Throwable cause = this.getCause();
		
		if ((cause != null) && java.sql.SQLException.class.isInstance(cause))
		{
			java.sql.SQLException exception = (java.sql.SQLException) cause;
			
			return exception.getSQLState();
		}
		
		return super.getSQLState();
	}
}
