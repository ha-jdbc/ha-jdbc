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

import java.sql.Connection;
import java.util.List;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public interface SynchronizationStrategy
{
	/**
	 * Synchronizes the an inactive database with an active database using the specified connections.
	 * Implementors must not close the specified connections.
	 * @param inactiveConnection a connection to the inactive database
	 * @param activeConnection a connection to the active database
	 * @param tableList a list of every table in the database
	 * @param descriptor the descriptor of this database cluster
	 * @throws java.sql.SQLException if synchronization fails
	 */
	public void synchronize(Connection inactiveConnection, Connection activeConnection, List tableList, DatabaseClusterDescriptor descriptor) throws java.sql.SQLException;
}
