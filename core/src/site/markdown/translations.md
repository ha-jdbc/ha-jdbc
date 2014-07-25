# Translations

As of version 3.1, HA-JDBC supports internationalization of its log messages and exceptions via the GNU [gettext] utilities.
Translations of messages are provided to HA-JDBC at build time via gettext PO files.
During compilation, these will be converted into the appropriate resource bundles to be used at runtime.

To build HA-JDBC with i18n support, use the "i18n" maven profile.
e.g.

	mvn -P i18n install

## Create a new translation

To create a translation, use the following commands:

	cd core/src/main/po
	msginit

This will create a file named /LANG/.po (where /LANG/ is the locale of your machine) based on the ha-jdbc.pot template file.
You can then edit this file using your favorite PO file editor.
For details, see http://www.gnu.org/software/gettext/manual/gettext.html#Creating

Please submit a github pull request to add your new translations into HA-JDBC.

## Modify an existing translation

Existing translations for HA-JDBC can be found in the src/main/po directory of the core sub-project.
The easiest way to edit a translation is with a PO file editor.
See http://www.gnu.org/software/gettext/manual/gettext.html#Editing

Please submit a github pull request to merge your translation updates/fixes into HA-JDBC.

[gettext]: http://www.gnu.org/software/gettext/