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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import net.sf.hajdbc.DatabaseClusterDescriptor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author  Paul Ferraro
 * @version $Revision$
 * @since   1.0
 */
public class ForeignKey
{
	private static Log log = LogFactory.getLog(ForeignKey.class);
	
	private String name;
	private String table;
	private String column;
	private String foreignTable;
	private String foreignColumn;
	
	private String formatSQL(String pattern)
	{
		return MessageFormat.format(pattern, new Object[] { this.name, this.table, this.column, this.foreignTable, this.foreignColumn });
	}
	
	public boolean equals(Object object)
	{
		ForeignKey foreignKey = (ForeignKey) object;
		
		return (foreignKey != null) && (foreignKey.name != null) && foreignKey.name.equals(this.name);
	}
	
	public int hashCode()
	{
		return this.name.hashCode();
	}
	
	public static Collection collectForeignKeys(Connection connection, List tableList) throws SQLException
	{
		List foreignKeyList = new LinkedList();
		
		Iterator tables = tableList.iterator();
		
		while (tables.hasNext())
		{
			String table = (String) tables.next();
			
			ResultSet resultSet = connection.getMetaData().getImportedKeys(null, null, table);
			
			while (resultSet.next())
			{
				ForeignKey foreignKey = new ForeignKey();
				
				foreignKey.table = table;
				foreignKey.name = resultSet.getString("FK_NAME");
				foreignKey.column = resultSet.getString("FKCOLUMN_NAME");
				foreignKey.foreignTable = resultSet.getString("PKTABLE_NAME");
				foreignKey.foreignColumn = resultSet.getString("PKCOLUMN_NAME");
	
				foreignKeyList.add(foreignKey);
			}
			
			resultSet.close();
		}
		
		return foreignKeyList;
	}

	private static void executeSQL(Connection connection, Collection foreignKeyCollection, String sqlPattern) throws java.sql.SQLException
	{
		Statement statement = connection.createStatement();
		
		Iterator foreignKeys = foreignKeyCollection.iterator();
		
		while (foreignKeys.hasNext())
		{
			ForeignKey foreignKey = (ForeignKey) foreignKeys.next();
			
			String sql = foreignKey.formatSQL(sqlPattern);
			
			if (log.isDebugEnabled())
			{
				log.debug(sql);
			}
			
			statement.execute(sql);
		}

		statement.close();
	}
	
	public static void drop(Connection connection, Collection foreignKeyCollection, DatabaseClusterDescriptor descriptor) throws java.sql.SQLException
	{
		executeSQL(connection, foreignKeyCollection, descriptor.getDropForeignKeySQL());
	}
	
	public static void create(Connection connection, Collection foreignKeyCollection, DatabaseClusterDescriptor descriptor) throws java.sql.SQLException
	{
		executeSQL(connection, foreignKeyCollection, descriptor.getCreateForeignKeySQL());
	}
}
