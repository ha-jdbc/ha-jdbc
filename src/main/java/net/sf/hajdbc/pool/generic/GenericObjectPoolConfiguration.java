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
package net.sf.hajdbc.pool.generic;

import org.apache.commons.pool.impl.GenericObjectPool;

/**
 * Expose getters/setters to {@link GenericObjectPool.Config} properties.
 * @author Paul Ferraro
 */
public class GenericObjectPoolConfiguration extends GenericObjectPool.Config
{
	public int getMaxIdle()
	{
		return maxIdle;
	}
	public void setMaxIdle(int maxIdle)
	{
		this.maxIdle = maxIdle;
	}
	public int getMinIdle()
	{
		return minIdle;
	}
	public void setMinIdle(int minIdle)
	{
		this.minIdle = minIdle;
	}
	public int getMaxActive()
	{
		return maxActive;
	}
	public void setMaxActive(int maxActive)
	{
		this.maxActive = maxActive;
	}
	public long getMaxWait()
	{
		return maxWait;
	}
	public void setMaxWait(long maxWait)
	{
		this.maxWait = maxWait;
	}
	public byte getWhenExhaustedAction()
	{
		return whenExhaustedAction;
	}
	public void setWhenExhaustedAction(byte whenExhaustedAction)
	{
		this.whenExhaustedAction = whenExhaustedAction;
	}
	public boolean isTestOnBorrow()
	{
		return testOnBorrow;
	}
	public void setTestOnBorrow(boolean testOnBorrow)
	{
		this.testOnBorrow = testOnBorrow;
	}
	public boolean isTestOnReturn()
	{
		return testOnReturn;
	}
	public void setTestOnReturn(boolean testOnReturn)
	{
		this.testOnReturn = testOnReturn;
	}
	public boolean isTestWhileIdle()
	{
		return testWhileIdle;
	}
	public void setTestWhileIdle(boolean testWhileIdle)
	{
		this.testWhileIdle = testWhileIdle;
	}
	public long getTimeBetweenEvictionRunsMillis()
	{
		return timeBetweenEvictionRunsMillis;
	}
	public void setTimeBetweenEvictionRunsMillis(long timeBetweenEvictionRunsMillis)
	{
		this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
	}
	public int getNumTestsPerEvictionRun()
	{
		return numTestsPerEvictionRun;
	}
	public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun)
	{
		this.numTestsPerEvictionRun = numTestsPerEvictionRun;
	}
	public long getMinEvictableIdleTimeMillis()
	{
		return minEvictableIdleTimeMillis;
	}
	public void setMinEvictableIdleTimeMillis(long minEvictableIdleTimeMillis)
	{
		this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
	}
	public long getSoftMinEvictableIdleTimeMillis()
	{
		return softMinEvictableIdleTimeMillis;
	}
	public void setSoftMinEvictableIdleTimeMillis(long softMinEvictableIdleTimeMillis)
	{
		this.softMinEvictableIdleTimeMillis = softMinEvictableIdleTimeMillis;
	}
	public boolean isLifo()
	{
		return lifo;
	}
	public void setLifo(boolean lifo)
	{
		this.lifo = lifo;
	}
}
