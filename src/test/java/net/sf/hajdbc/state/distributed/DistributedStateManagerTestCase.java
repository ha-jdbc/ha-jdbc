/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2014  Paul Ferraro
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
package net.sf.hajdbc.state.distributed;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Map;
import java.util.UUID;

import net.sf.hajdbc.Database;
import net.sf.hajdbc.DatabaseCluster;
import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.distributed.CommandDispatcherFactory;
import net.sf.hajdbc.distributed.jgroups.JGroupsCommandDispatcherFactory;
import net.sf.hajdbc.durability.Durability;
import net.sf.hajdbc.durability.Durability.Phase;
import net.sf.hajdbc.durability.InvocationEvent;
import net.sf.hajdbc.durability.InvocationEventImpl;
import net.sf.hajdbc.durability.InvokerEvent;
import net.sf.hajdbc.durability.InvokerEventImpl;
import net.sf.hajdbc.durability.InvokerResult;
import net.sf.hajdbc.durability.InvokerResultImpl;
import net.sf.hajdbc.state.DatabaseEvent;
import net.sf.hajdbc.state.StateManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for {@link DistributedStateManager}.
 * @author Paul Ferraro
 */
public class DistributedStateManagerTestCase
{
	private DatabaseCluster<Void, Database<Void>> cluster1 = mock(DatabaseCluster.class);
	private DatabaseCluster<Void, Database<Void>> cluster2 = mock(DatabaseCluster.class);
	private StateManager localManager1 = mock(StateManager.class);
	private StateManager localManager2 = mock(StateManager.class);
	private Durability<Void, Database<Void>> durability1 = mock(Durability.class);
	private Durability<Void, Database<Void>> durability2 = mock(Durability.class);
	private DistributedStateManager<Void, Database<Void>> manager1;
	private DistributedStateManager<Void, Database<Void>> manager2;

	@Before
	public void init() throws Exception
	{
		String id = "cluster";

		when(this.cluster1.getId()).thenReturn(id);
		when(this.cluster2.getId()).thenReturn(id);
		when(this.cluster1.getStateManager()).thenReturn(this.localManager1);
		when(this.cluster2.getStateManager()).thenReturn(this.localManager2);
		when(this.cluster1.getDurability()).thenReturn(this.durability1);
		when(this.cluster2.getDurability()).thenReturn(this.durability2);
		
		CommandDispatcherFactory dispatcherFactory1 = createCommandDispatcherFactory("node1");
		CommandDispatcherFactory dispatcherFactory2 = createCommandDispatcherFactory("node2");

		this.manager1 = new DistributedStateManager<Void, Database<Void>>(this.cluster1, dispatcherFactory1);
		this.manager1.start();
		
		verify(this.localManager1).start();
		
		this.manager2 = new DistributedStateManager<Void, Database<Void>>(this.cluster2, dispatcherFactory2);
		this.manager2.start();
		
		verify(this.localManager2).start();
	}

	static CommandDispatcherFactory createCommandDispatcherFactory(String name)
	{
		JGroupsCommandDispatcherFactory factory = new JGroupsCommandDispatcherFactory();
		factory.setName(name);
		factory.setStack("fast-local.xml");
		return factory;
	}

	@After
	public void destroy()
	{
		this.manager1.stop();
		
		verify(this.localManager1).stop();
		
		this.manager1 = null;
		
		this.manager2.stop();
		
		verify(this.localManager2).stop();
		
		this.manager2 = null;
	}

	@Test
	public void activate()
	{
		String databaseId = "test";
		Database<Void> database = mock(Database.class);

		when(database.getId()).thenReturn(databaseId);
		
		DatabaseEvent event = new DatabaseEvent(database);
		
		when(this.cluster2.getDatabase(databaseId)).thenReturn(database);
		
		this.manager1.activated(event);
		
		verify(this.localManager1).activated(event);
		verify(this.cluster2).activate(database, this.localManager2);
		
		when(this.cluster1.getDatabase(databaseId)).thenReturn(database);
		
		this.manager2.activated(event);
		
		verify(this.localManager2).activated(event);
		verify(this.cluster1).activate(database, this.localManager1);
	}

	@Test
	public void deactivate()
	{
		String databaseId = "test";
		Database<Void> database = mock(Database.class);

		when(database.getId()).thenReturn(databaseId);
		
		DatabaseEvent event = new DatabaseEvent(database);
		
		when(this.cluster2.getDatabase(databaseId)).thenReturn(database);
		
		this.manager1.deactivated(event);
		
		verify(this.localManager1).deactivated(event);
		verify(this.cluster2).deactivate(database, this.localManager2);
		
		when(this.cluster1.getDatabase(databaseId)).thenReturn(database);
		
		this.manager2.deactivated(event);
		
		verify(this.localManager2).deactivated(event);
		verify(this.cluster1).deactivate(database, this.localManager1);
	}

	@Test
	public void recover()
	{
		Map<InvocationEvent, Map<String, InvokerEvent>> invocations = mock(Map.class);
		
		when(this.localManager1.recover()).thenReturn(invocations);
		
		Map<InvocationEvent, Map<String, InvokerEvent>> result = this.manager1.recover();
		
		assertSame(invocations, result);
		
		when(this.localManager2.recover()).thenReturn(invocations);
		
		result = this.manager2.recover();
		
		assertSame(invocations, result);
	}

	@Test
	public void durability()
	{
		Object tx = UUID.randomUUID();
		Phase phase = Phase.COMMIT;
		ExceptionType exceptionType = ExceptionType.SQL;
		InvocationEvent invocationEvent = new InvocationEventImpl(tx, phase, exceptionType);
		
		this.manager1.beforeInvocation(invocationEvent);
		
		verify(this.localManager1).beforeInvocation(invocationEvent);
		
		Map<InvocationEvent, Map<String, InvokerEvent>> invocations = this.manager2.getRemoteInvokers(this.manager1);
		
		assertEquals(1, invocations.size());
		Map<String, InvokerEvent> invokers = invocations.get(invocationEvent);
		assertNotNull(invokers);
		assertEquals(0, invokers.size());
		
		String databaseId = "test";
		InvokerEvent invokerEvent = new InvokerEventImpl(tx, phase, databaseId);
		
		this.manager1.beforeInvoker(invokerEvent);
		
		verify(this.localManager1).beforeInvoker(invokerEvent);
		
		invocations = this.manager2.getRemoteInvokers(this.manager1);
		
		assertEquals(1, invocations.size());
		invokers = invocations.get(invocationEvent);
		assertNotNull(invokers);
		assertEquals(1, invokers.size());
		assertEquals(invokerEvent, invokers.get(databaseId));
		
		InvokerResult result = new InvokerResultImpl("value");
		invokerEvent.setResult(result);
		
		this.manager1.afterInvoker(invokerEvent);
		
		verify(this.localManager1).afterInvoker(invokerEvent);
		
		invocations = this.manager2.getRemoteInvokers(this.manager1);
		
		assertEquals(1, invocations.size());
		invokers = invocations.get(invocationEvent);
		assertNotNull(invokers);
		assertEquals(1, invokers.size());
		InvokerEvent resultEvent = invokers.get(databaseId);
		assertEquals(invokerEvent, resultEvent);
		assertEquals(result.getValue(), resultEvent.getResult().getValue());
		
		this.manager1.afterInvocation(invocationEvent);
		
		verify(this.localManager1).afterInvocation(invocationEvent);
		
		invocations = this.manager2.getRemoteInvokers(this.manager1);
		
		assertEquals(0, invocations.size());
	}
}
