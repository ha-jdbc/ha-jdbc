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
package net.sf.hajdbc;

import java.sql.DriverManager;
import java.util.Properties;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.Reference;
import javax.sql.DataSource;

import net.sf.hajdbc.sql.MockDriver;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.testng.annotations.Configuration;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public abstract class DatabaseClusterTestCase
{
	protected IMocksControl control = EasyMock.createStrictControl();
	protected Context context;
	private MBeanServer server;
	
	@Configuration(beforeTestClass = true)
	public void setUp() throws Exception
	{
		this.server = MBeanServerFactory.createMBeanServer();
		
		DriverManager.registerDriver(new MockDriver());
		
		Properties properties = new Properties();
		
		properties.setProperty(Context.INITIAL_CONTEXT_FACTORY, "net.sf.hajdbc.sql.MockInitialContextFactory");
		
		this.context = new InitialContext(properties);
		
		Reference reference = new Reference(DataSource.class.toString(), "net.sf.hajdbc.sql.MockDataSourceFactory", null);
		
		this.context.rebind("datasource1", reference);
		this.context.rebind("datasource2", reference);
	}

	@Configuration(afterTestClass = true)
	public void tearDown() throws Exception
	{
		DriverManager.deregisterDriver(new MockDriver());
		
		this.context.unbind("datasource1");
		this.context.unbind("datasource2");
		
		MBeanServerFactory.releaseMBeanServer(this.server);
	}
	
	@Configuration(afterTestMethod = true)
	public void reset()
	{
		this.control.reset();
	}
}
