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
package net.sf.hajdbc.cache;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.UniqueConstraint;

/**
 * @author Paul Ferraro
 *
 */
public class LazyTableProperties implements TableProperties
{
	private String schema;
	private String table;
	private DatabaseMetaDataSupport support;
	private Map<String, ColumnProperties> columnMap;
	private UniqueConstraint primaryKey;
	private Collection<UniqueConstraint> uniqueConstraints;
	private Collection<ForeignKeyConstraint> foreignKeyConstraints;
	private String name;
	
	public LazyTableProperties(DatabaseMetaDataSupport support, String schema, String table)
	{
		this.schema = schema;
		this.table = table;
		this.support = support;
	}
	
	/**
	 * @see net.sf.hajdbc.TableProperties#getColumns()
	 */
	public synchronized Collection<String> getColumns() throws SQLException
	{
		if (this.columnMap == null)
		{
			this.columnMap = this.support.getColumns(LazyDatabaseMetaDataCache.getDatabaseMetaData(), this.schema, this.table);
		}
		
		return this.columnMap.keySet();
	}

	/**
	 * @see net.sf.hajdbc.TableProperties#getColumn(java.lang.String)
	 */
	public synchronized ColumnProperties getColumn(String column) throws SQLException
	{
		if (this.columnMap == null)
		{
			this.columnMap = this.support.getColumns(LazyDatabaseMetaDataCache.getDatabaseMetaData(), this.schema, this.table);
		}
		
		return this.columnMap.get(column);
	}

	/**
	 * @see net.sf.hajdbc.TableProperties#getPrimaryKey()
	 */
	public synchronized UniqueConstraint getPrimaryKey() throws SQLException
	{
		if (this.primaryKey == null)
		{
			this.primaryKey = this.support.getPrimaryKey(LazyDatabaseMetaDataCache.getDatabaseMetaData(), this.schema, this.table);
		}
		
		return this.primaryKey;
	}

	/**
	 * @see net.sf.hajdbc.TableProperties#getForeignKeyConstraints()
	 */
	public synchronized Collection<ForeignKeyConstraint> getForeignKeyConstraints() throws SQLException
	{
		if (this.foreignKeyConstraints == null)
		{
			this.foreignKeyConstraints = this.support.getForeignKeyConstraints(LazyDatabaseMetaDataCache.getDatabaseMetaData(), this.schema, this.table);
		}
		
		return this.foreignKeyConstraints;
	}

	/**
	 * @see net.sf.hajdbc.TableProperties#getUniqueConstraints()
	 */
	public synchronized Collection<UniqueConstraint> getUniqueConstraints() throws SQLException
	{
		if (this.uniqueConstraints == null)
		{
			this.uniqueConstraints = this.support.getUniqueConstraints(LazyDatabaseMetaDataCache.getDatabaseMetaData(), this.schema, this.table);
		}
		
		return this.uniqueConstraints;
	}

	/**
	 * @see net.sf.hajdbc.TableProperties#getName()
	 */
	public synchronized String getName() throws SQLException
	{
		if (this.name == null)
		{
			this.name = this.support.getQualifiedNameForDML(LazyDatabaseMetaDataCache.getDatabaseMetaData(), this.schema, this.table);
		}
		
		return this.name;
	}
}
