/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.cache.lazy;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import net.sf.hajdbc.ColumnProperties;
import net.sf.hajdbc.ColumnPropertiesFactory;
import net.sf.hajdbc.ForeignKeyConstraint;
import net.sf.hajdbc.ForeignKeyConstraintFactory;
import net.sf.hajdbc.IdentifierNormalizer;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.QualifiedNameFactory;
import net.sf.hajdbc.UniqueConstraint;
import net.sf.hajdbc.UniqueConstraintFactory;
import net.sf.hajdbc.cache.AbstractTableProperties;
import net.sf.hajdbc.cache.DatabaseMetaDataProvider;
import net.sf.hajdbc.dialect.Dialect;

/**
 * @author Paul Ferraro
 *
 */
public class LazyTableProperties extends AbstractTableProperties
{
	private final DatabaseMetaDataProvider metaDataProvider;
	private final Dialect dialect;
	private final UniqueConstraintFactory uniqueConstraintFactory;
	private final ForeignKeyConstraintFactory foreignKeyConstraintFactory;
	private final ColumnPropertiesFactory columnPropertiesFactory;

	private final AtomicReference<Map<String, ColumnProperties>> columnsRef = new AtomicReference<Map<String, ColumnProperties>>();
	private final AtomicReference<UniqueConstraint> primaryKeyRef = new AtomicReference<UniqueConstraint>();
	private final AtomicReference<Collection<UniqueConstraint>> uniqueConstraintsRef = new AtomicReference<Collection<UniqueConstraint>>();
	private final AtomicReference<Collection<ForeignKeyConstraint>> foreignKeyConstraintsRef = new AtomicReference<Collection<ForeignKeyConstraint>>();
	private final AtomicReference<Collection<String>> identityColumnsRef = new AtomicReference<Collection<String>>();
	
	public LazyTableProperties(QualifiedName table, DatabaseMetaDataProvider metaDataProvider, Dialect dialect, QualifiedNameFactory nameFactory)
	{
		super(table);
		
		this.metaDataProvider = metaDataProvider;
		this.dialect = dialect;
		IdentifierNormalizer normalizer = nameFactory.getIdentifierNormalizer();
		this.uniqueConstraintFactory = dialect.createUniqueConstraintFactory(normalizer);
		this.foreignKeyConstraintFactory = dialect.createForeignKeyConstraintFactory(nameFactory);
		this.columnPropertiesFactory = dialect.createColumnPropertiesFactory(normalizer);
	}

	@Override
	protected Map<String, ColumnProperties> getColumnMap() throws SQLException
	{
		Map<String, ColumnProperties> columns = this.columnsRef.get();
		
		if (columns == null)
		{
			columns = this.dialect.getColumns(this.metaDataProvider.getDatabaseMetaData(), this.getName(), this.columnPropertiesFactory);
			
			if (!this.columnsRef.compareAndSet(null, columns))
			{
				return this.columnsRef.get();
			}
		}
		
		return columns;
	}
	
	/**
	 * @see net.sf.hajdbc.TableProperties#getPrimaryKey()
	 */
	@Override
	public UniqueConstraint getPrimaryKey() throws SQLException
	{
		UniqueConstraint primaryKey = this.primaryKeyRef.get();
		
		if (primaryKey == null)
		{
			primaryKey = this.dialect.getPrimaryKey(this.metaDataProvider.getDatabaseMetaData(), this.getName(), this.uniqueConstraintFactory);
			
			if (!this.primaryKeyRef.compareAndSet(null, primaryKey))
			{
				return this.primaryKeyRef.get();
			}
		}
		
		return primaryKey;
	}

	/**
	 * @see net.sf.hajdbc.TableProperties#getForeignKeyConstraints()
	 */
	@Override
	public Collection<ForeignKeyConstraint> getForeignKeyConstraints() throws SQLException
	{
		Collection<ForeignKeyConstraint> foreignKeyConstraints = this.foreignKeyConstraintsRef.get();
		
		if (foreignKeyConstraints == null)
		{
			foreignKeyConstraints = this.dialect.getForeignKeyConstraints(this.metaDataProvider.getDatabaseMetaData(), this.getName(), this.foreignKeyConstraintFactory);
			
			if (!this.foreignKeyConstraintsRef.compareAndSet(null, foreignKeyConstraints))
			{
				return this.foreignKeyConstraintsRef.get();
			}
		}
		
		return foreignKeyConstraints;
	}

	/**
	 * @see net.sf.hajdbc.TableProperties#getUniqueConstraints()
	 */
	@Override
	public Collection<UniqueConstraint> getUniqueConstraints() throws SQLException
	{
		Collection<UniqueConstraint> uniqueConstraints = this.uniqueConstraintsRef.get();
		
		if (uniqueConstraints == null)
		{
			uniqueConstraints = this.dialect.getUniqueConstraints(this.metaDataProvider.getDatabaseMetaData(), this.getName(), this.getPrimaryKey(), this.uniqueConstraintFactory);
			
			if (!this.uniqueConstraintsRef.compareAndSet(null, uniqueConstraints))
			{
				return this.uniqueConstraintsRef.get();
			}
		}
		
		return uniqueConstraints;
	}

	/**
	 * @see net.sf.hajdbc.TableProperties#getIdentityColumns()
	 */
	@Override
	public Collection<String> getIdentityColumns() throws SQLException
	{
		Collection<String> identityColumns = this.identityColumnsRef.get();
		
		if (identityColumns == null)
		{
			identityColumns = this.dialect.getIdentityColumns(this.getColumnMap().values());
			
			if (!this.identityColumnsRef.compareAndSet(null, identityColumns))
			{
				return this.identityColumnsRef.get();
			}
		}
		
		return identityColumns;
	}
}
