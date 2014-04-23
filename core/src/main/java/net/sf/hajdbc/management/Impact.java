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
package net.sf.hajdbc.management;

import javax.management.MBeanOperationInfo;

/**
 * @author Paul Ferraro
 */
public enum Impact
{
	ACTION(MBeanOperationInfo.ACTION),
	ACTION_INFO(MBeanOperationInfo.ACTION_INFO),
	INFO(MBeanOperationInfo.INFO),
	UNKNOWN(MBeanOperationInfo.UNKNOWN);
	
	private final int code;
	
	private Impact(int code)
	{
		this.code = code;
	}
	
	public static Impact forCode(int code)
	{
		for (Impact impact: Impact.values())
		{
			if (impact.code == code) return impact;
		}
		
		throw new IllegalArgumentException(code + " is not a valid mbean operation impact code");
	}
	
	public int getCode()
	{
		return this.code;
	}
}
