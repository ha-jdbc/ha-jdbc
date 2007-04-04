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
package net.sf.hajdbc.sql;

import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Map;
import java.util.TreeMap;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.MockDatabase;
import net.sf.hajdbc.util.reflect.ProxyFactory;

import org.easymock.EasyMock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author Paul Ferraro
 *
 */
@SuppressWarnings("unchecked")
public class TestSavepoint implements Savepoint
{
	private DatabaseCluster cluster = EasyMock.createStrictMock(DatabaseCluster.class);
	private Savepoint savepoint1 = EasyMock.createStrictMock(java.sql.Savepoint.class);
	private Savepoint savepoint2 = EasyMock.createStrictMock(java.sql.Savepoint.class);
	private SQLProxy parent = EasyMock.createStrictMock(SQLProxy.class);
	
	private Database database1 = new MockDatabase("1");
	private Database database2 = new MockDatabase("2");
	private Savepoint savepoint;
	
	@BeforeClass
	void init() throws Exception
	{
		Map<Database, Savepoint> map = new TreeMap<Database, Savepoint>();
		map.put(this.database1, this.savepoint1);
		map.put(this.database2, this.savepoint2);
		
		EasyMock.expect(this.parent.getDatabaseCluster()).andReturn(this.cluster);
		
		this.parent.addChild(EasyMock.isA(SavepointInvocationHandler.class));
		
		this.replay();
		
		this.savepoint = ProxyFactory.createProxy(Savepoint.class, new SavepointInvocationHandler(null, this.parent, EasyMock.createMock(Invoker.class), map));
		
		this.verify();
		this.reset();
	}
	
	private Object[] objects()
	{
		return new Object[] { this.cluster, this.savepoint1, this.savepoint2, this.parent };
	}
	
	void replay()
	{
		EasyMock.replay(this.objects());
	}
	
	void verify()
	{
		EasyMock.verify(this.objects());
	}
	
	@AfterMethod
	void reset()
	{
		EasyMock.reset(this.objects());
	}

	/**
	 * @see java.sql.Savepoint#getSavepointId()
	 */
	@Test
	public int getSavepointId() throws SQLException
	{
		EasyMock.expect(this.savepoint1.getSavepointId()).andReturn(1);
		
		this.replay();

		int result = this.savepoint.getSavepointId();
		
		this.verify();
		
		assert result == 1;
		
		return result;
	}

	/**
	 * @see java.sql.Savepoint#getSavepointName()
	 */
	@Test
	public String getSavepointName() throws SQLException
	{
		EasyMock.expect(this.savepoint1.getSavepointName()).andReturn("");
		
		this.replay();

		String result = this.savepoint.getSavepointName();
		
		this.verify();
		
		assert result.equals("");
		
		return result;
	}

}
