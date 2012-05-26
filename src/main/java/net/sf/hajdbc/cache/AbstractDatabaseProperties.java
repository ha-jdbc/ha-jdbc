/*
 * HA-JDBC: High-Availability JDBC
 * Copyright 2004-2009 Paul Ferraro
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
package net.sf.hajdbc.cache;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.hajdbc.DatabaseProperties;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.TableProperties;

/**
 * @author Paul Ferraro
 *
 */
public abstract class AbstractDatabaseProperties implements DatabaseProperties
{
	private final DatabaseMetaDataSupport support;
	private final boolean supportsSelectForUpdate;
	private final boolean locatorsUpdateCopy;
	
	public AbstractDatabaseProperties(DatabaseMetaData metaData, DatabaseMetaDataSupport support) throws SQLException
	{
		this.support = support;
		this.supportsSelectForUpdate = metaData.supportsSelectForUpdate();
		this.locatorsUpdateCopy = metaData.locatorsUpdateCopy();
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseProperties#supportsSelectForUpdate()
	 */
	@Override
	public final boolean supportsSelectForUpdate() throws SQLException
	{
		return this.supportsSelectForUpdate;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseProperties#locatorsUpdateCopy()
	 */
	@Override
	public boolean locatorsUpdateCopy() throws SQLException
	{
		return this.locatorsUpdateCopy;
	}

	/**
	 * @see net.sf.hajdbc.DatabaseProperties#getTables()
	 */
	@Override
	public final Collection<TableProperties> getTables() throws SQLException
	{
		return this.tables().values();
	}

	protected abstract Map<QualifiedName, TableProperties> tables() throws SQLException;

	/**
	 * @see net.sf.hajdbc.DatabaseProperties#getSequences()
	 */
	@Override
	public final Collection<SequenceProperties> getSequences() throws SQLException
	{
		return this.sequences().values();
	}

	protected abstract Map<QualifiedName, SequenceProperties> sequences() throws SQLException;
	
	/**
	 * @see net.sf.hajdbc.DatabaseProperties#findTable(java.lang.String)
	 */
	@Override
	public final TableProperties findTable(String table) throws SQLException
	{
		return this.support.find(this.tables(), table, this.defaultSchemas());
	}

	/**
	 * @see net.sf.hajdbc.DatabaseProperties#findSequence(java.lang.String)
	 */
	@Override
	public final SequenceProperties findSequence(String sequence) throws SQLException
	{
		return this.support.find(this.sequences(), sequence, this.defaultSchemas());
	}

	protected abstract List<String> defaultSchemas() throws SQLException;
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseProperties#findType(int, int[])
	 */
	@Override
	public String findType(int precision, int... types) throws SQLException
	{
		Map<Integer, Map.Entry<String, Integer>> map = this.types();

		for (int type: types)
		{
			Map.Entry<String, Integer> entry = map.get(type);
			if (entry != null)
			{
				String name = entry.getKey();
				Integer maxPrecision = entry.getValue();
				if (maxPrecision != null)
				{
					String qualifier = "(" + ((precision == 0) ? maxPrecision : precision) + ")";
					return name.contains("()") ? name.replace("()", qualifier) : name + qualifier;
				}
				
				return name;
			}
		}
		throw new SQLException("No native type found for " + Arrays.asList(types));
	}
	
	protected abstract Map<Integer, Entry<String, Integer>> types() throws SQLException;
}
