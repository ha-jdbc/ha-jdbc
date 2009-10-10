/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2008 Paul Ferraro
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
import java.util.List;
import java.util.Map;

import net.sf.hajdbc.Dialect;

/**
 * @author Paul Ferraro
 *
 */
public abstract class AbstractDatabaseProperties implements DatabaseProperties
{
	protected final Dialect dialect;
	protected final DatabaseMetaDataSupport support;
	
	private final boolean supportsSelectForUpdate;
	private final boolean locatorsUpdateCopy;
	
	protected AbstractDatabaseProperties(DatabaseMetaData metaData, DatabaseMetaDataSupportFactory factory, Dialect dialect) throws SQLException
	{
		this.dialect = dialect;
		this.support = factory.createSupport(metaData, dialect);
		this.supportsSelectForUpdate = metaData.supportsSelectForUpdate();
		this.locatorsUpdateCopy = metaData.locatorsUpdateCopy();
	}
	
	/**
	 * @see net.sf.hajdbc.cache.DatabaseProperties#supportsSelectForUpdate()
	 */
	@Override
	public final boolean supportsSelectForUpdate() throws SQLException
	{
		return this.supportsSelectForUpdate;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.cache.DatabaseProperties#locatorsUpdateCopy()
	 */
	@Override
	public boolean locatorsUpdateCopy() throws SQLException
	{
		return this.locatorsUpdateCopy;
	}

	/**
	 * @see net.sf.hajdbc.cache.DatabaseProperties#getTables()
	 */
	@Override
	public final Collection<TableProperties> getTables() throws SQLException
	{
		return this.getTableMap().values();
	}

	/**
	 * @see net.sf.hajdbc.cache.DatabaseProperties#getSequences()
	 */
	@Override
	public final Collection<SequenceProperties> getSequences() throws SQLException
	{
		return this.getSequenceMap().values();
	}
	
	/**
	 * @see net.sf.hajdbc.cache.DatabaseProperties#findTable(java.lang.String)
	 */
	@Override
	public final TableProperties findTable(String table) throws SQLException
	{
		return this.support.find(this.getTableMap(), table, this.getDefaultSchemaList());
	}

	/**
	 * @see net.sf.hajdbc.cache.DatabaseProperties#findSequence(java.lang.String)
	 */
	@Override
	public final SequenceProperties findSequence(String sequence) throws SQLException
	{
		return this.support.find(this.getSequenceMap(), sequence, this.getDefaultSchemaList());
	}

	protected abstract Map<String, TableProperties> getTableMap() throws SQLException;
	
	protected abstract Map<String, SequenceProperties> getSequenceMap() throws SQLException;
	
	protected abstract List<String> getDefaultSchemaList() throws SQLException;
}
