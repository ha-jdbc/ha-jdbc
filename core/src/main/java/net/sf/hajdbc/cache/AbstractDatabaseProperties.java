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
package net.sf.hajdbc.cache;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.hajdbc.DatabaseProperties;
import net.sf.hajdbc.QualifiedName;
import net.sf.hajdbc.QualifiedNameFactory;
import net.sf.hajdbc.SequenceProperties;
import net.sf.hajdbc.TableProperties;
import net.sf.hajdbc.dialect.Dialect;

/**
 * @author Paul Ferraro
 *
 */
public abstract class AbstractDatabaseProperties implements DatabaseProperties
{
	protected final QualifiedNameFactory nameFactory;
	private final boolean supportsSelectForUpdate;
	private final boolean locatorsUpdateCopy;
	
	public AbstractDatabaseProperties(DatabaseMetaData metaData, Dialect dialect) throws SQLException
	{
		this.supportsSelectForUpdate = metaData.supportsSelectForUpdate();
		this.locatorsUpdateCopy = metaData.locatorsUpdateCopy();
		this.nameFactory = dialect.createQualifiedNameFactory(metaData, dialect.createIdentifierNormalizer(metaData));
	}
	
	/**
	 * @see net.sf.hajdbc.DatabaseProperties#supportsSelectForUpdate()
	 */
	@Override
	public final boolean supportsSelectForUpdate()
	{
		return this.supportsSelectForUpdate;
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.DatabaseProperties#locatorsUpdateCopy()
	 */
	@Override
	public boolean locatorsUpdateCopy()
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
		return this.find(this.tables(), table, this.defaultSchemas());
	}

	/**
	 * @see net.sf.hajdbc.DatabaseProperties#findSequence(java.lang.String)
	 */
	@Override
	public final SequenceProperties findSequence(String sequence) throws SQLException
	{
		return this.find(this.sequences(), sequence, this.defaultSchemas());
	}
	
	private <T> T find(Map<QualifiedName, T> map, String raw, List<String> defaultSchemaList)
	{
		QualifiedName name = this.nameFactory.parse(raw);

		T properties = map.get(name);
		
		if ((properties == null) && (name.getSchema() == null))
		{
			Iterator<String> schemas = defaultSchemaList.iterator();
			while ((properties == null) && schemas.hasNext())
			{
				properties = map.get(this.nameFactory.createQualifiedName(schemas.next(), raw));
			}
		}

		return properties;
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
		throw new IllegalStateException();
	}
	
	protected abstract Map<Integer, Entry<String, Integer>> types() throws SQLException;
}
