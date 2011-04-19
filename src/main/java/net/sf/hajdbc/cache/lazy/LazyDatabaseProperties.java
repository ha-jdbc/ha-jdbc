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
package net.sf.hajdbc.cache.lazy;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;

import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.cache.AbstractDatabaseProperties;
import net.sf.hajdbc.cache.DatabaseMetaDataProvider;
import net.sf.hajdbc.cache.DatabaseMetaDataSupport;
import net.sf.hajdbc.cache.QualifiedName;
import net.sf.hajdbc.cache.SequenceProperties;
import net.sf.hajdbc.cache.TableProperties;

/**
 * @author Paul Ferraro
 *
 */
public class LazyDatabaseProperties extends AbstractDatabaseProperties
{
	private final DatabaseMetaDataSupport support;
	private final Dialect dialect;
	private final DatabaseMetaDataProvider provider;
	
	private final AtomicReference<Map<String, TableProperties>> tablesRef = new AtomicReference<Map<String, TableProperties>>();
	private final AtomicReference<Map<String, SequenceProperties>> sequencesRef = new AtomicReference<Map<String, SequenceProperties>>();
	private final AtomicReference<List<String>> defaultSchemasRef = new AtomicReference<List<String>>();
	private final AtomicReference<Map<Integer, Map.Entry<String, Integer>>> typesRef = new AtomicReference<Map<Integer, Map.Entry<String, Integer>>>();
	
	public LazyDatabaseProperties(DatabaseMetaDataProvider provider, DatabaseMetaDataSupport support, Dialect dialect) throws SQLException
	{
		super(provider.getDatabaseMetaData(), support);
		
		this.provider = provider;
		this.support = support;
		this.dialect = dialect;
	}
	
	@Override
	protected Map<String, TableProperties> tables() throws SQLException
	{
		Map<String, TableProperties> tables = this.tablesRef.get();
		
		if (tables == null)
		{
			tables = new HashMap<String, TableProperties>();
			
			for (QualifiedName table: this.support.getTables(this.provider.getDatabaseMetaData()))
			{
				TableProperties properties = new LazyTableProperties(this.provider, this.support, table);
				
				tables.put(properties.getName(), properties);
			}
			
			if (!this.tablesRef.compareAndSet(null, tables))
			{
				return this.tablesRef.get();
			}
		}
		
		return tables;
	}
	
	@Override
	protected Map<String, SequenceProperties> sequences() throws SQLException
	{
		Map<String, SequenceProperties> sequences = this.sequencesRef.get();

		if (sequences == null)
		{
			sequences = new HashMap<String, SequenceProperties>();
			
			for (SequenceProperties sequence: this.support.getSequences(this.provider.getDatabaseMetaData()))
			{
				sequences.put(sequence.getName(), sequence);
			}
			
			if (!this.sequencesRef.compareAndSet(null, sequences))
			{
				return this.sequencesRef.get();
			}
		}

		return sequences;
	}
	
	@Override
	protected List<String> defaultSchemas() throws SQLException
	{
		List<String> schemas = this.defaultSchemasRef.get();
		
		if (schemas == null)
		{
			schemas = this.dialect.getDefaultSchemas(this.provider.getDatabaseMetaData());
			
			if (!this.defaultSchemasRef.compareAndSet(null, schemas))
			{
				return this.defaultSchemasRef.get();
			}
		}
		
		return schemas;
	}

	@Override
	protected Map<Integer, Entry<String, Integer>> types() throws SQLException
	{
		Map<Integer, Map.Entry<String, Integer>> types = this.typesRef.get();
		
		if (types == null)
		{
			types = this.support.getTypes(this.provider.getDatabaseMetaData());
			
			if (!this.typesRef.compareAndSet(null, types))
			{
				return this.typesRef.get();
			}
		}
		
		return types;
	}
}
