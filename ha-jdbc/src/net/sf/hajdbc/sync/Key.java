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
package net.sf.hajdbc.sync;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

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
	protected String table;
	
	protected Key(String name, String schema, String table, String quote)
	{
		this.name = quote + name + quote;
		
		StringBuilder builder = new StringBuilder();
		
		if (schema != null)
		{
			builder.append(quote).append(schema).append(quote).append(".");
		}
		
		this.table = builder.append(quote).append(table).append(quote).toString();
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
	 * @param keys a Collection of Key objects
	 * @param sqlPattern a sql pattern
	 * @throws SQLException if a database error occurs
	 */
	public static void executeSQL(Connection connection, Collection<? extends Key> keys, String sqlPattern) throws SQLException
	{
		Statement statement = connection.createStatement();
		
		for (Key key: keys)
		{
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
