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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.cache.lazy.LazyTableProperties;
import net.sf.hajdbc.util.ref.VolatileReference;

/**
 * @author Paul Ferraro
 *
 */
public abstract class AbstractLazyDatabaseProperties extends AbstractDatabaseProperties implements DatabaseMetaDataProvider
{
	private final VolatileReference<Map<String, TableProperties>> tableMapRef = new VolatileReference<Map<String, TableProperties>>();
	private final VolatileReference<Map<String, SequenceProperties>> sequenceMapRef = new VolatileReference<Map<String, SequenceProperties>>();
	private final VolatileReference<List<String>> defaultSchemaListRef = new VolatileReference<List<String>>();
	
	protected AbstractLazyDatabaseProperties(DatabaseMetaData metaData, DatabaseMetaDataSupportFactory factory, Dialect dialect) throws SQLException
	{
		super(metaData, factory, dialect);
	}
	
	@Override
	protected Map<String, TableProperties> getTableMap() throws SQLException
	{
		synchronized (this.tableMapRef)
		{
			Map<String, TableProperties> map = this.tableMapRef.get();
			
			if (map == null)
			{
				map = new HashMap<String, TableProperties>();
				
				for (QualifiedName table: this.support.getTables(this.getDatabaseMetaData()))
				{
					TableProperties properties = new LazyTableProperties(this, this.support, table);
					
					map.put(properties.getName(), properties);
				}
				
				this.tableMapRef.set(map);
			}
			
			return map;
		}
	}
	
	@Override
	protected Map<String, SequenceProperties> getSequenceMap() throws SQLException
	{
		synchronized (this.sequenceMapRef)
		{
			Map<String, SequenceProperties> map = this.sequenceMapRef.get();
			
			if (map == null)
			{
				map = new HashMap<String, SequenceProperties>();
				
				for (SequenceProperties sequence: this.support.getSequences(this.getDatabaseMetaData()))
				{
					map.put(sequence.getName(), sequence);
				}
				
				this.sequenceMapRef.set(map);
			}
			
			return map;
		}
	}
	
	@Override
	protected List<String> getDefaultSchemaList() throws SQLException
	{
		synchronized (this.defaultSchemaListRef)
		{
			List<String> list = this.defaultSchemaListRef.get();
			
			if (list == null)
			{
				list = this.dialect.getDefaultSchemas(this.getDatabaseMetaData());
				
				this.defaultSchemaListRef.set(list);
			}
			
			return list;
		}
	}
}
