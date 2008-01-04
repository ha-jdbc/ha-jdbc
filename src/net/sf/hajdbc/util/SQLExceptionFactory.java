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
package net.sf.hajdbc.util;

import java.sql.SQLException;

/**
 * @author Paul Ferraro
 *
 */
public final class SQLExceptionFactory
{
	/**
	 * Helper method for missing SQLException(String, Throwable) constructor in Java < 1.6
	 * @param message the description of the exception
	 * @param cause the exception to wrap
	 * @return an SQLException wrapping the given cause
	 */
	public static SQLException createSQLException(String message, Throwable cause)
	{
		SQLException exception = new SQLException(message);
		
		exception.initCause(cause);
		
		return exception;
	}
	
	/**
	 * Helper method for missing SQLException(Throwable) constructor in Java < 1.6
	 * @param cause the exception to wrap
	 * @return an SQLException wrapping the given cause
	 */
	public static SQLException createSQLException(Throwable cause)
	{
		if (cause instanceof SQLException)
		{
			return (SQLException) cause;
		}
		
		SQLException exception = new SQLException();
		
		exception.initCause(cause);
		
		return exception;
	}
}
