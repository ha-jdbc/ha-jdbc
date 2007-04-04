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
package net.sf.hajdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface for retrieving pre-processed, cached, database meta data.
 * 
 * @author Paul Ferraro
 * @since 2.0
 */
public interface DatabaseMetaDataCache
{
	/**
	 * Initializes/Flushes this cache.
	 * @throws SQLException if flush fails
	 */
	public void flush(Connection connection) throws SQLException;
	
	/**
	 * Retrieves processed meta data for this database.
	 * @return a DatabaseProperties implementation
	 * @throws SQLException if database properties could not be fetched.
	 */
	public DatabaseProperties getDatabaseProperties(Connection connection) throws SQLException;
	
	public void setDialect(Dialect dialect);
}