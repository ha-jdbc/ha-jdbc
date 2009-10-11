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
package net.sf.hajdbc.sql.xa;

import javax.naming.StringRefAddr;
import javax.sql.XADataSource;

import net.sf.hajdbc.sql.CommonDataSourceReference;

/**
 * @author Paul Ferraro
 *
 */
public class XADataSourceReference extends CommonDataSourceReference<XADataSource>
{
	public static final String FORCE_2PC = "force2PC";
	
	private static final long serialVersionUID = -1333879245815171042L;
	
	/**
	 * Constructs a reference to an <code>XADataSource</code> for the specified cluster
	 * @param cluster a cluster identifier
	 */
	public XADataSourceReference(String cluster)
	{
		this(cluster, null);
	}

	/**
	 * Constructs a reference to an <code>XADataSource</code> for the specified cluster
	 * @param cluster a cluster identifier
	 * @param config the uri of the configuration file
	 */
	public XADataSourceReference(String cluster, String config)
	{
		this(cluster, config, false);
	}

	public XADataSourceReference(String cluster, boolean force2PC)
	{
		this(cluster, null, force2PC);
	}
	
	public XADataSourceReference(String cluster, String config, boolean force2PC)
	{
		super(XADataSource.class, XADataSourceFactory.class, cluster, config);

		this.add(new StringRefAddr(FORCE_2PC, Boolean.toString(force2PC)));
	}
}
