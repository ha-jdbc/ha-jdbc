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
import net.sf.hajdbc.util.ref.VolatileReference;

/**
 * @author Paul Ferraro
 *
 */
public class LazyTableProperties extends AbstractTableProperties
{
	private final DatabaseMetaDataProvider metaDataProvider;
	private final QualifiedName table;
	private final DatabaseMetaDataSupport support;
	
	private final VolatileReference<Map<String, ColumnProperties>> columnMapRef = new VolatileReference<Map<String, ColumnProperties>>();
	private final VolatileReference<UniqueConstraint> primaryKeyRef = new VolatileReference<UniqueConstraint>();
	private final VolatileReference<Collection<UniqueConstraint>> uniqueConstraintsRef = new VolatileReference<Collection<UniqueConstraint>>();
	private final VolatileReference<Collection<ForeignKeyConstraint>> foreignKeyConstraintsRef = new VolatileReference<Collection<ForeignKeyConstraint>>();
	private final VolatileReference<Collection<String>> identityColumnsRef = new VolatileReference<Collection<String>>();
	
	public LazyTableProperties(DatabaseMetaDataProvider metaDataProvider, DatabaseMetaDataSupport support, QualifiedName table)
	{
		super(support, table);
		
		this.metaDataProvider = metaDataProvider;
		this.support = support;
		this.table = table;
	}

	@Override
	protected Map<String, ColumnProperties> getColumnMap() throws SQLException
	{
		synchronized (this.columnMapRef)
		{
			Map<String, ColumnProperties> map = this.columnMapRef.get();
			
			if (map == null)
			{
				map = this.support.getColumns(this.metaDataProvider.getDatabaseMetaData(), this.table);
				
				this.columnMapRef.set(map);
			}
			
			return map;
		}
	}
	
	/**
	 * @see net.sf.hajdbc.TableProperties#getPrimaryKey()
	 */
	@Override
	public UniqueConstraint getPrimaryKey() throws SQLException
	{
		synchronized (this.primaryKeyRef)
		{
			UniqueConstraint key = this.primaryKeyRef.get();
			
			if (key == null)
			{
				key = this.support.getPrimaryKey(this.metaDataProvider.getDatabaseMetaData(), this.table);
				
				this.primaryKeyRef.set(key);
			}
			
			return key;
		}
	}

	/**
	 * @see net.sf.hajdbc.TableProperties#getForeignKeyConstraints()
	 */
	@Override
	public Collection<ForeignKeyConstraint> getForeignKeyConstraints() throws SQLException
	{
		synchronized (this.foreignKeyConstraintsRef)
		{
			Collection<ForeignKeyConstraint> keys = this.foreignKeyConstraintsRef.get();
			
			if (keys == null)
			{
				keys = this.support.getForeignKeyConstraints(this.metaDataProvider.getDatabaseMetaData(), this.table);
				
				this.foreignKeyConstraintsRef.set(keys);
			}
			
			return keys;
		}
	}

	/**
	 * @see net.sf.hajdbc.TableProperties#getUniqueConstraints()
	 */
	@Override
	public Collection<UniqueConstraint> getUniqueConstraints() throws SQLException
	{
		synchronized (this.uniqueConstraintsRef)
		{
			Collection<UniqueConstraint> keys = this.uniqueConstraintsRef.get();
			
			if (keys == null)
			{
				keys = this.support.getUniqueConstraints(this.metaDataProvider.getDatabaseMetaData(), this.table, this.getPrimaryKey());
				
				this.uniqueConstraintsRef.set(keys);
			}
			
			return keys;
		}
	}

	/**
	 * @see net.sf.hajdbc.TableProperties#getIdentityColumns()
	 */
	@Override
	public Collection<String> getIdentityColumns() throws SQLException
	{
		synchronized (this.identityColumnsRef)
		{
			Collection<String> columns = this.identityColumnsRef.get();
			
			if (columns == null)
			{
				columns = this.support.getIdentityColumns(this.getColumnMap().values());
				
				this.identityColumnsRef.set(columns);
			}
			
			return columns;
		}
	}
}
