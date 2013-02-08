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

import java.util.ResourceBundle;
import java.util.regex.Pattern;

import net.sf.hajdbc.util.Strings;

/**
 * @author Paul Ferraro
 */
public class Version
{
	public static final Version CURRENT = new Version(ResourceBundle.getBundle(Version.class.getName()).getString("version"));
	
	private final String version;
	private final int major;
	private final int minor;
	private final int revision;
	
	Version(String version)
	{
		this.version = version;
		this.major = parse(0);
		this.minor = parse(1);
		this.revision = parse(2);
	}
	
	private int parse(int index)
	{
		return Integer.parseInt(this.version.split(Pattern.quote(Strings.DASH))[0].split(Pattern.quote(Strings.DOT))[index]);
	}
	
	public int getMajor()
	{
		return this.major;
	}
	
	public int getMinor()
	{
		return this.minor;
	}
	
	public int getRevision()
	{
		return this.revision;
	}
	
	public String toString()
	{
		return this.version;
	}
}
