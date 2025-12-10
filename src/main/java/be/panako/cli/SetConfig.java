/***************************************************************************
*                                                                          *
* Panako - acoustic fingerprinting                                         *
*                                                                          *
****************************************************************************/

package be.panako.cli;

import be.panako.util.Config;
import be.panako.util.Key;

/**
 * CLI command to set a configuration key to a value and persist it.
 * Usage: panako set-config KEY VALUE
 */
class Set extends Application {

    @Override
    public void run(String... args) {
        if (args.length < 2) {
            System.err.println("Usage: panako set KEY VALUE");
            return;
        }
        String keyName = args[0];
        String value = args[1];
        try {
            Key k = Key.valueOf(keyName);
            Config.set(k, value);
            Config.persist();
            System.out.printf("Set %s=%s and persisted to config.properties\n", keyName, value);
        } catch (IllegalArgumentException e) {
            System.err.printf("Unknown key '%s'.\n", keyName);
        }
    }

    @Override
    public String description() {
        return "Sets and persists a configuration key";
    }

    @Override
    public String synopsis() {
        return "KEY VALUE";
    }

    @Override
    public boolean needsStorage() {
        return false;
    }

    @Override
    public boolean writesToStorage() {
        return false;
    }
}
