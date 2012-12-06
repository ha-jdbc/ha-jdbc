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
package net.sf.hajdbc.state;

import static org.junit.Assert.*;
import org.junit.Test;

import net.sf.hajdbc.MockDatabase;
import net.sf.hajdbc.util.Objects;

/**
 * @author Paul Ferraro
 */
public class DatabaseEventTest
{
	@Test
	public void serialization()
	{
		DatabaseEvent event1 = new DatabaseEvent(new MockDatabase("1"));
		DatabaseEvent event2 = Objects.deserialize(Objects.serialize(event1));
		assertEquals(event1, event2);
	}
}
