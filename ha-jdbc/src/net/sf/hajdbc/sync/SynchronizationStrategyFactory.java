/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2004 Paul Ferraro
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
package net.sf.hajdbc.sync;

import org.jibx.runtime.IUnmarshallingContext;
import org.jibx.runtime.impl.UnmarshallingContext;

import net.sf.hajdbc.SynchronizationStrategy;

/**
 * @author  Paul Ferraro
 * @since   1.0
 */
public class SynchronizationStrategyFactory
{
	private static final String CLASS = "class";
	
	/**
	 * Factory method for creating synchronization strategies
	 * @param ctx the current unmarshalling context
	 * @return a new synchronization strategy implementation
	 * @throws Exception if unmarshalling fails
	 */
	public static SynchronizationStrategy createSynchronizationStrategy(IUnmarshallingContext ctx) throws Exception
	{
		UnmarshallingContext context = (UnmarshallingContext) ctx;
		
		String className = context.attributeText(null, CLASS);
		
		return (SynchronizationStrategy) Class.forName(className).newInstance();
	}
	
	private SynchronizationStrategyFactory()
	{
	}
}
