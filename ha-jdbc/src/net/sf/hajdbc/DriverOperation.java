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

import java.sql.Driver;
import java.sql.SQLException;

/**
 * @author Paul Ferraro
 * @version $Revision$
 */
public abstract class DriverOperation implements Operation
{
	/**
	 * Helper method that simplifies operation interface for the HA-JDBC Driver.
	 * @param database a database descriptor
	 * @param driver a database driver
	 * @return the result from executing this operation
	 * @throws SQLException if execution fails
	 */
	public abstract Object execute(DriverDatabase database, Driver driver) throws SQLException;
	
	/**
	 * @see net.sf.hajdbc.Operation#execute(net.sf.hajdbc.Database, java.lang.Object)
	 */
	public final Object execute(Database database, Object driver) throws SQLException
	{
		return this.execute((DriverDatabase) database, (Driver) driver);
	}
}