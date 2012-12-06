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
import net.sf.hajdbc.ExceptionType;
import net.sf.hajdbc.util.Objects;

import org.junit.Test;

public class InvocationEventTest
{
	@Test
	public void serialization()
	{
		InvocationEvent event1 = new InvocationEventImpl(10, Durability.Phase.COMMIT, ExceptionType.SQL);
		InvocationEvent event2 = Objects.deserialize(Objects.serialize(event1));
		assertEquals(event1, event2);
		assertEquals(event1.getTransactionId(), event2.getTransactionId());
		assertEquals(event1.getPhase(), event2.getPhase());
		assertEquals(event1.getExceptionType(), event2.getExceptionType());
	}
}
