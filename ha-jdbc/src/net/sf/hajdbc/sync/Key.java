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
package net.sf.hajdbc.sync;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public abstract class Key
{
	private static Log log = LogFactory.getLog(Key.class);
	
	protected String name;
	protected String tablePrefix;
	protected String table;
	
	protected Key(String name, String schema, String table, String quote)
	{
		this.name = quote + name + quote;
		this.tablePrefix = (schema != null) ? quote + schema + quote + "." : "";
		this.table = quote + table + quote;
	}
	
	protected abstract String formatSQL(String pattern);

	/**
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object object)
	{
		Key key = (Key) object;
		
		return (key != null) && (key.name != null) && key.name.equals(this.name);
	}
	
	/**
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode()
	{
		return this.name.hashCode();
	}
	
	/**
	 * For each foreign key in the specified collection, generates and executes sql statements using the specified pattern and the specified connection.
	 * @param connection a database connection
	 * @param keyCollection a Collection<ForeignKey>
	 * @param sqlPattern a sql pattern
	 * @throws SQLException if a database error occurs
	 */
	public static void executeSQL(Connection connection, Collection keyCollection, String sqlPattern) throws SQLException
	{
		Statement statement = connection.createStatement();
		
		Iterator keys = keyCollection.iterator();
		
		while (keys.hasNext())
		{
			Key key = (Key) keys.next();
			
			String sql = key.formatSQL(sqlPattern);
			
			if (log.isDebugEnabled())
			{
				log.debug(sql);
			}
			
			statement.execute(sql);
		}

		statement.close();
	}
}
