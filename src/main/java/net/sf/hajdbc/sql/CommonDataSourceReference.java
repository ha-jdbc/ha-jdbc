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
package net.sf.hajdbc.sql;

import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.naming.spi.ObjectFactory;

/**
 * @author Paul Ferraro
 * @param <Z> 
 */
public abstract class CommonDataSourceReference<Z> extends Reference
{
	private static final long serialVersionUID = 1408239702660701511L;
	
	protected static final String CLUSTER = "cluster"; //$NON-NLS-1$
	protected static final String CONFIG = "config"; //$NON-NLS-1$
	
	/**
	 * Constructs a reference for a DataSource implementation.
	 * @param targetClass the target class of the DataSource.
	 * @param factoryClass the ObjectFactory class for creating a DataSource
	 * @param cluster a cluster identifier
	 * @param config the uri of the configuration file
	 */
	protected CommonDataSourceReference(Class<Z> targetClass, Class<? extends ObjectFactory> factoryClass, String cluster, String config)
	{
		super(targetClass.getName(), factoryClass.getName(), null);
		
		if (cluster == null)
		{
			throw new IllegalArgumentException();
		}
		
		this.add(new StringRefAddr(CLUSTER, cluster));
		
		if (config != null)
		{
			this.add(new StringRefAddr(CONFIG, config));
		}
	}
}
