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
package net.sf.hajdbc.sql.pool.xa;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.Operation;
import net.sf.hajdbc.SQLException;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public abstract class XAResourceOperation implements Operation
{
	/**
	 * @param database
	 * @param resource
	 * @return the return value of the XAResource operation
	 * @throws XAException
	 */
	public abstract Object execute(XADataSourceDatabase database, XAResource resource) throws XAException;
	
	/**
	 * @see net.sf.hajdbc.Operation#execute(net.sf.hajdbc.Database, java.lang.Object)
	 */
	public Object execute(Database database, Object resource) throws SQLException
	{
		try
		{
			return this.execute((XADataSourceDatabase) database, (XAResource) resource);
		}
		catch (XAException e)
		{
			throw new SQLException(e);
		}
	}
}
