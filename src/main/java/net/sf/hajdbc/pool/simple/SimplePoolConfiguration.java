/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (c) 2004-2009 Paul Ferraro
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
package net.sf.hajdbc.pool.simple;

/**
 * @author paul
 *
 */
public class SimplePoolConfiguration
{
	private int capacity = 10;
	private int initSize = 1;
	private boolean enforceMax = false;
	
	/**
	 * @return the capacity
	 */
	public int getCapacity()
	{
		return this.capacity;
	}
	/**
	 * @param capacity the capacity to set
	 */
	public void setCapacity(int capacity)
	{
		this.capacity = capacity;
	}
	/**
	 * @return the initSize
	 */
	public int getInitSize()
	{
		return this.initSize;
	}
	/**
	 * @param initSize the initSize to set
	 */
	public void setInitSize(int initSize)
	{
		this.initSize = initSize;
	}
	/**
	 * @return the enforceMax
	 */
	public boolean isEnforceMax()
	{
		return this.enforceMax;
	}
	/**
	 * @param enforceMax the enforceMax to set
	 */
	public void setEnforceMax(boolean enforceMax)
	{
		this.enforceMax = enforceMax;
	}
}
