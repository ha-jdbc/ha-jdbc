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
package net.sf.hajdbc.sql;

import java.util.concurrent.ExecutorService;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;

import net.sf.hajdbc.util.concurrent.SynchronousExecutor;

/**
 * @author Paul Ferraro
 *
 */
@XmlEnum(String.class)
public enum TransactionMode
{
	@XmlEnumValue("parallel")
	PARALLEL(false),
	@XmlEnumValue("serial")
	SERIAL(true);

	private final boolean synchronous;
	
	private TransactionMode(boolean synchronous)
	{
		this.synchronous = synchronous;
	}
	
	public ExecutorService getTransactionExecutor(ExecutorService executor)
	{
		return this.synchronous ? new SynchronousExecutor(executor) : executor;
	}
}
