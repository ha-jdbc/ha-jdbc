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
package net.sf.hajdbc.pool.xa;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class XAException extends javax.transaction.xa.XAException
{
	/**
	 * Constructs a new XAException.
	 * @param message
	 */
	public XAException(String message)
	{
		super(message);
	}

	/**
	 * Constructs a new XAException.
	 * @param message
	 * @param cause 
	 */
	public XAException(String message, Throwable cause)
	{
		super(message);
		this.initCause(cause);
	}

	/**
	 * Constructs a new XAException.
	 * @param cause
	 */
	public XAException(Throwable cause)
	{
		super();
		this.initCause(cause);
	}
}
