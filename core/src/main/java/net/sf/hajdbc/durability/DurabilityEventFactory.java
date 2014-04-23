package net.sf.hajdbc.durability;

import net.sf.hajdbc.ExceptionType;

public interface DurabilityEventFactory
{
	DurabilityEvent createEvent(Object transactionId, Durability.Phase phase);

	InvocationEvent createInvocationEvent(Object transactionId, Durability.Phase phase, ExceptionType exceptionType);

	InvokerEvent createInvokerEvent(Object transactionId, Durability.Phase phase, String databaseId);
}
