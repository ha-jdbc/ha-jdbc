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
package net.sf.hajdbc.sql;

import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.naming.spi.ObjectFactory;

/**
 * @author Paul Ferraro
 * @param <D> 
 */
public abstract class CommonDataSourceReference<D> extends Reference
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
	protected CommonDataSourceReference(Class<D> targetClass, Class<? extends ObjectFactory> factoryClass, String cluster, String config)
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
