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

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.UniqueConstraint;

/**
 * @author Paul Ferraro
 *
 */
public class EagerTableProperties extends AbstractTableProperties
{
	private Map<String, ColumnProperties> columnMap;
	private UniqueConstraint primaryKey;
	private Collection<UniqueConstraint> uniqueConstraints;
	private Collection<ForeignKeyConstraint> foreignKeyConstraints;
	private String name;
	
	public EagerTableProperties(DatabaseMetaData metaData, DatabaseMetaDataSupport support, String schema, String table) throws SQLException
	{
		this.columnMap = support.getColumns(metaData, schema, table);
		this.primaryKey = support.getPrimaryKey(metaData, schema, table);
		this.uniqueConstraints = support.getUniqueConstraints(metaData, schema, table);
		this.foreignKeyConstraints = support.getForeignKeyConstraints(metaData, schema, table);
		this.name = support.getQualifiedNameForDML(schema, table);
	}

	/**
	 * @see net.sf.hajdbc.TableProperties#getColumns()
	 */
	public Collection<String> getColumns()
	{
		return this.columnMap.keySet();
	}

	/**
	 * @see net.sf.hajdbc.TableProperties#getColumnProperties(java.lang.String)
	 */
	public ColumnProperties getColumnProperties(String column)
	{
		return this.columnMap.get(column);
	}

	/**
	 * @see net.sf.hajdbc.TableProperties#getPrimaryKey()
	 */
	public UniqueConstraint getPrimaryKey()
	{
		return this.primaryKey;
	}

	/**
	 * @see net.sf.hajdbc.TableProperties#getForeignKeyConstraints()
	 */
	public Collection<ForeignKeyConstraint> getForeignKeyConstraints()
	{
		return this.foreignKeyConstraints;
	}

	/**
	 * @see net.sf.hajdbc.TableProperties#getUniqueConstraints()
	 */
	public Collection<UniqueConstraint> getUniqueConstraints()
	{
		return this.uniqueConstraints;
	}

	/**
	 * @see net.sf.hajdbc.TableProperties#getName()
	 */
	public String getName()
	{
		return this.name;
	}
}
