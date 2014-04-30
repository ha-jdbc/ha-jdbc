package net.sf.hajdbc.messages.i18n;

import net.sf.hajdbc.messages.simple.SimpleMessages;

import org.xnap.commons.i18n.I18n;
import org.xnap.commons.i18n.I18nFactory;

public class I18nMessages extends SimpleMessages
{
	private final I18n i18n;

	public I18nMessages(String bundle)
	{
		this.i18n = I18nFactory.getI18n(SimpleMessages.class, bundle);
	}
	
	@Override
	protected String tr(String message)
	{
		return this.i18n.tr(message);
	}

	@Override
	protected String tr(String pattern, Object... args)
	{
		return this.i18n.tr(pattern, args);
	}
}
