/*
 * Copyright (c) 2004-2006, Identity Theft 911, LLC.  All rights reserved.
 */
package net.sf.hajdbc.local;

/**
 * @author Paul Ferraro
 *
 */
public enum Transaction
{
	LOCAL, XA;
	
	public static Transaction deserialize(String value)
	{
		return Transaction.valueOf(value.toUpperCase());
	}
	
	public static String serialize(Transaction transaction)
	{
		return transaction.name().toLowerCase();
	}
}
