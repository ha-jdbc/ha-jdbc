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
package net.sf.hajdbc.cache;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.UniqueConstraint;

/**
 * @author Paul Ferraro
 *
 */
public class LazyTableProperties extends AbstractTableProperties
{
	private QualifiedName table;
	private DatabaseMetaDataSupport support;
	private Map<String, ColumnProperties> columnMap;
	private UniqueConstraint primaryKey;
	private Collection<UniqueConstraint> uniqueConstraints;
	private Collection<ForeignKeyConstraint> foreignKeyConstraints;
	private String name;
	
	public LazyTableProperties(DatabaseMetaDataSupport support, QualifiedName table)
	{
		this.table = table;
		this.support = support;
	}
	
	/**
	 * @see net.sf.hajdbc.TableProperties#getColumns()
	 */
	@Override
	public synchronized Collection<String> getColumns() throws SQLException
	{
		return this.getColumnMap().keySet();
	}

	/**
	 * @see net.sf.hajdbc.TableProperties#getColumnProperties(java.lang.String)
	 */
	@Override
	public synchronized ColumnProperties getColumnProperties(String column) throws SQLException
	{
		return this.getColumnMap().get(column);
	}

	private synchronized Map<String, ColumnProperties> getColumnMap() throws SQLException
	{
		if (this.columnMap == null)
		{
			this.columnMap = this.support.getColumns(LazyDatabaseProperties.getDatabaseMetaData(), this.table);
		}
		
		return this.columnMap;
	}
	
	/**
	 * @see net.sf.hajdbc.TableProperties#getPrimaryKey()
	 */
	@Override
	public synchronized UniqueConstraint getPrimaryKey() throws SQLException
	{
		if (this.primaryKey == null)
		{
			this.primaryKey = this.support.getPrimaryKey(LazyDatabaseProperties.getDatabaseMetaData(), this.table);
		}
		
		return this.primaryKey;
	}

	/**
	 * @see net.sf.hajdbc.TableProperties#getForeignKeyConstraints()
	 */
	@Override
	public synchronized Collection<ForeignKeyConstraint> getForeignKeyConstraints() throws SQLException
	{
		if (this.foreignKeyConstraints == null)
		{
			this.foreignKeyConstraints = this.support.getForeignKeyConstraints(LazyDatabaseProperties.getDatabaseMetaData(), this.table);
		}
		
		return this.foreignKeyConstraints;
	}

	/**
	 * @see net.sf.hajdbc.TableProperties#getUniqueConstraints()
	 */
	@Override
	public synchronized Collection<UniqueConstraint> getUniqueConstraints() throws SQLException
	{
		if (this.uniqueConstraints == null)
		{
			this.uniqueConstraints = this.support.getUniqueConstraints(LazyDatabaseProperties.getDatabaseMetaData(), this.table);
		}
		
		return this.uniqueConstraints;
	}

	/**
	 * @see net.sf.hajdbc.TableProperties#getName()
	 */
	@Override
	public synchronized String getName()
	{
		if (this.name == null)
		{
			this.name = this.support.qualifyNameForDML(this.table);
		}
		
		return this.name;
	}

	/**
	 * @see net.sf.hajdbc.TableProperties#getIdentityColumns()
	 */
	@Override
	public Collection<String> getIdentityColumns() throws SQLException
	{
		return null;
	}
}
