package net.sf.hajdbc.state.distributed;

import java.io.Serializable;

import net.sf.hajdbc.durability.InvokerEvent;

public interface InvokerDescriptor extends Serializable
{
	InvokerEvent getEvent();
}
