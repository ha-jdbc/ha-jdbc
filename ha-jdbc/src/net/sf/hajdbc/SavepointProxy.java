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

import java.sql.SQLException;
import java.sql.Savepoint;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class SavepointProxy extends SQLProxy implements Savepoint
{
	public SavepointProxy(ConnectionProxy connection, ConnectionOperation operation) throws SQLException
	{
		super(connection, operation);
	}
	
	/**
	 * @see java.sql.Savepoint#getSavepointId()
	 */
	public int getSavepointId() throws SQLException
	{
		SavepointOperation operation = new SavepointOperation()
		{
			public Object execute(Savepoint savepoint) throws SQLException
			{
				return Integer.valueOf(savepoint.getSavepointId());
			}
		};
		
		return ((Integer) super.executeGet(operation)).intValue();
	}

	/**
	 * @see java.sql.Savepoint#getSavepointName()
	 */
	public String getSavepointName() throws SQLException
	{
		SavepointOperation operation = new SavepointOperation()
		{
			public Object execute(Savepoint savepoint) throws SQLException
			{
				return savepoint.getSavepointName();
			}
		};
		
		return (String) super.executeGet(operation);
	}
}
