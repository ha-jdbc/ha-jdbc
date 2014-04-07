/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
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
package net.sf.hajdbc.durability;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.util.UUID;

import net.sf.hajdbc.util.Objects;

import org.junit.Test;

/**
 * @author Paul Ferraro
 *
 */
public class InvokerEventTest
{
	@Test
	public void serializationNoResult()
	{
		InvokerEvent event1 = new InvokerEventImpl(UUID.randomUUID(), Durability.Phase.COMMIT, "1");
		InvokerEvent event2 = Objects.deserialize(Objects.serialize(event1), InvokerEvent.class);
		assertEquals(event1, event2);
		assertEquals(event1.getTransactionId(), event2.getTransactionId());
		assertEquals(event1.getPhase(), event2.getPhase());
		assertEquals(event1.getDatabaseId(), event2.getDatabaseId());
	}

	@Test
	public void serializationWithResult()
	{
		InvokerEvent event1 = new InvokerEventImpl(UUID.randomUUID(), Durability.Phase.COMMIT, "1");
		event1.setResult(new InvokerResultImpl(100));
		InvokerEvent event2 = Objects.deserialize(Objects.serialize(event1), InvokerEvent.class);
		assertEquals(event1, event2);
		assertEquals(event1.getTransactionId(), event2.getTransactionId());
		assertEquals(event1.getPhase(), event2.getPhase());
		assertEquals(event1.getDatabaseId(), event2.getDatabaseId());
		assertEquals(event1.getResult().getValue(), event2.getResult().getValue());
	}

	@Test
	public void serializationWithException()
	{
		InvokerEvent event1 = new InvokerEventImpl(UUID.randomUUID(), Durability.Phase.COMMIT, "1");
		event1.setResult(new InvokerResultImpl(new Exception()));
		InvokerEvent event2 = Objects.deserialize(Objects.serialize(event1), InvokerEvent.class);
		assertEquals(event1, event2);
		assertEquals(event1.getTransactionId(), event2.getTransactionId());
		assertEquals(event1.getPhase(), event2.getPhase());
		assertEquals(event1.getDatabaseId(), event2.getDatabaseId());
		assertNotNull(event2.getResult().getException());
	}

	@Test
	public void equals() {
		UUID txId = UUID.randomUUID();
		InvokerEvent event1 = new InvokerEventImpl(txId, Durability.Phase.COMMIT, "1");
		InvokerEvent event2 = new InvokerEventImpl(txId, Durability.Phase.COMMIT, "1");
		assertEquals(event1, event2);
		
		event1.setResult(new InvokerResultImpl(0));
		assertEquals(event1, event2);
		
		event2.setResult(new InvokerResultImpl(new Exception()));
		assertEquals(event1, event2);
		
		event2 = new InvokerEventImpl(txId, Durability.Phase.COMMIT, "2");
		assertNotEquals(event1, event2);
		
		event2 = new InvokerEventImpl(txId, Durability.Phase.ROLLBACK, "1");
		assertNotEquals(event1, event2);
		
		event2 = new InvokerEventImpl(UUID.randomUUID(), Durability.Phase.COMMIT, "1");
		assertNotEquals(event1, event2);
	}
}
