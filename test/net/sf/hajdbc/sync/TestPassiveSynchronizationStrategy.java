/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2006 Paul Ferraro
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
package net.sf.hajdbc.sync;

import java.sql.Connection;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import net.sf.hajdbc.DatabaseMetaDataCache;
import net.sf.hajdbc.Dialect;
import net.sf.hajdbc.SynchronizationStrategy;

/**
 * @author Paul Ferraro
 *
 */
public class TestPassiveSynchronizationStrategy implements SynchronizationStrategy
{
	private IMocksControl control = EasyMock.createStrictControl();
	
	private SynchronizationStrategy strategy = new PassiveSynchronizationStrategy();
	
	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#requiresTableLocking()
	 */
	@Test
	public boolean requiresTableLocking()
	{
		boolean requires = this.strategy.requiresTableLocking();
		
		assert !requires;
		
		return requires;
	}
	
	@DataProvider(name = "sync")
	public Object[][] syncProvider()
	{
		return new Object[][] { new Object[] { this.control.createMock(Connection.class), this.control.createMock(Connection.class), this.control.createMock(DatabaseMetaDataCache.class), this.control.createMock(Dialect.class) } };
	}

	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#synchronize(java.sql.Connection, java.sql.Connection, net.sf.hajdbc.DatabaseMetaDataCache, net.sf.hajdbc.Dialect)
	 */
	@Test(dataProvider = "sync")
	public void synchronize(Connection inactiveConnection, Connection activeConnection, DatabaseMetaDataCache metaData, Dialect dialect)
	{
		this.control.replay();
		
		this.control.verify();
		
		this.control.reset();
	}
}
