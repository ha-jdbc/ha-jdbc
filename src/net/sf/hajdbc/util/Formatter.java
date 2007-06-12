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
package net.sf.hajdbc.util;

import java.text.ParseException;
import java.util.Date;

import org.quartz.CronExpression;

/**
 * @author Paul Ferraro
 *
 */
public final class Formatter
{
	public static String serializeCronExpression(CronExpression expression)
	{
		return (expression != null) ? expression.getCronExpression() : null;
	}
	
	public static CronExpression deserializeCronExpression(String schedule) throws ParseException
	{
		if (schedule == null)
		{
			return null;
		}
		
		CronExpression expression = new CronExpression(schedule);
		
		// Quartz inappropriately throws an UnsupportedOperationException if both day-of-week
		// and day-of-month are specified - so we preemptively test the expression here.
		try
		{
			expression.getNextValidTimeAfter(new Date());
			
			return expression;
		}
		catch (UnsupportedOperationException e)
		{
			throw new ParseException(e.getMessage(), schedule.length());
		}
	}
	
	public static String serializeClass(Class<?> targetClass)
	{
		return (targetClass != null) ? targetClass.getName() : null;
	}
	
	public static Class<?> deserializeClass(String className) throws ClassNotFoundException
	{
		return (className != null) ? Thread.currentThread().getContextClassLoader().loadClass(className) : null;
	}
}
