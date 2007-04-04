/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2007 Paul Ferraro
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

import java.sql.SQLException;

import net.sf.hajdbc.SynchronizationContext;
import net.sf.hajdbc.SynchronizationStrategy;

import org.easymock.EasyMock;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
public class TestPassiveSynchronizationStrategy implements SynchronizationStrategy
{
	private SynchronizationStrategy strategy = new PassiveSynchronizationStrategy();
	
	@DataProvider(name = "context")
	public Object[][] contextProvider()
	{
		return new Object[][] { new Object[] { EasyMock.createStrictMock(SynchronizationContext.class) } };
	}

	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#cleanup(net.sf.hajdbc.sync.SynchronizationContextImpl)
	 */
	@Test(dataProvider = "context")
	public <D> void cleanup(SynchronizationContext<D> context)
	{
		EasyMock.replay(context);
		
		this.strategy.cleanup(context);
		
		EasyMock.verify(context);
	}

	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#prepare(net.sf.hajdbc.sync.SynchronizationContextImpl)
	 */
	@Test(dataProvider = "context")
	public <D> void prepare(SynchronizationContext<D> context) throws SQLException
	{
		EasyMock.replay(context);
		
		this.strategy.prepare(context);
		
		EasyMock.verify(context);
	}

	/**
	 * @see net.sf.hajdbc.SynchronizationStrategy#synchronize(java.sql.Connection, java.sql.Connection, net.sf.hajdbc.DatabaseMetaDataCache, net.sf.hajdbc.Dialect)
	 */
	@Test(dataProvider = "context")
	public <D> void synchronize(SynchronizationContext<D> context) throws SQLException
	{
		EasyMock.replay(context);
		
		this.strategy.synchronize(context);
		
		EasyMock.verify(context);
	}
}
