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
package net.sf.hajdbc.sql;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.util.Collections;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @param <E> 
 * @since   1.0
 */
public class ConnectionFactory<E> extends SQLObject<E, Void>
{
	/**
	 * Constructs a new ConnectionFactory.
	 * @param databaseCluster a database cluster
	 * @param targetClass target class of 
	 */
	public ConnectionFactory(DatabaseCluster databaseCluster, Class<E> targetClass)
	{
		super(databaseCluster, Collections.cast(databaseCluster.getConnectionFactoryMap(), Database.class, targetClass));
	}

	/**
	 * @see net.sf.hajdbc.sql.SQLObject#close(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected void close(Void parent, E object)
	{
		// Nothing to close
	}
}
