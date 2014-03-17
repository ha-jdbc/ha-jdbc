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
package net.sf.hajdbc;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class VersionTest
{
	@Test
	public void finalRelease()
	{
		parse(new Version("1.2.3"), 1, 2, 3);
	}

	@Test
	public void rc()
	{
		parse(new Version("1.2.3-rc-4"), 1, 2, 3);
	}
	
	@Test
	public void beta()
	{
		parse(new Version("1.2.3-beta-4"), 1, 2, 3);
	}
	
	@Test
	public void alpha()
	{
		parse(new Version("1.2.3-alpha-4"), 1, 2, 3);
	}
	
	@Test
	public void finalSnapshot()
	{
		parse(new Version("1.2.3-SNAPSHOT"), 1, 2, 3);
	}
	
	@Test
	public void betaSnapshot()
	{
		parse(new Version("1.2.3-beta-4-SNAPSHOT"), 1, 2, 3);
	}

	private static void parse(Version version, int major, int minor, int revision)
	{
		assertEquals(major, version.getMajor());
		assertEquals(minor, version.getMinor());
		assertEquals(revision, version.getRevision());
	}
}
