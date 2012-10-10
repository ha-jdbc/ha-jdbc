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
	private static final String VERSION = "version"; //$NON-NLS-1$
	
	private static final ResourceBundle resource = ResourceBundle.getBundle(Version.class.getName());
	
	/**
	 * Returns the current HA-JDBC version.
	 * @return a version label
	 */
	public static String getVersion()
	{
		return resource.getString(VERSION);
	}
	
	private static int getVersionPart(int index)
	{
		return Integer.parseInt(getVersion().split(Pattern.quote(Strings.DASH), 1)[0].split(Pattern.quote(Strings.DOT), index + 1)[index]);
	}
	
	public static int getMajorVersion()
	{
		return getVersionPart(0);
	}
	
	public static int getMinorVersion()
	{
		return getVersionPart(1);
	}
	
	public static int getRevisionVersion()
	{
		return getVersionPart(2);
	}
}
