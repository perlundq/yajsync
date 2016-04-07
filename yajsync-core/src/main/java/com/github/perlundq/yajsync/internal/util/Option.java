/*
 * Argument parsing option type
 *
 * Copyright (C) 2013, 2014 Per Lundqvist
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.github.perlundq.yajsync.internal.util;

public class Option
{
    public interface Handler
    {
        ArgumentParser.Status handle(Option option) throws ArgumentParsingError;
    }

    public static abstract class ContinuingHandler implements Handler
    {
        @Override
        public ArgumentParser.Status handle(Option option)
            throws ArgumentParsingError
        {
            handleAndContinue(option);
            return ArgumentParser.Status.CONTINUE;
        }

        public abstract void handleAndContinue(Option option)
            throws ArgumentParsingError;
    }

    public enum Policy { OPTIONAL, REQUIRED }

    private final Class<?> _type;
    private final Policy _policy;
    private final String _longName;
    private final String _shortName;
    private final String _shortHelp;
    private final Handler _handler;
    private Object _value;
    private int _numInstances = 0;

    private Option(Class<?> type,
                   Policy policy,
                   String longName,
                   String shortName,
                   String shortHelp,
                   Handler handler)
    {
        assert type != null;
        assert policy != null;
        assert longName != null;
        assert shortName != null;
        assert shortHelp != null;
        assert handler != null ||
               (type == Void.class && policy == Policy.REQUIRED);
        assert ! (longName.isEmpty() && shortName.isEmpty());
        assert longName.length() > 1 || shortName.length() == 1 :
            "An option must have either a long name (>=2 characters) and/or a" +
            " one character long short name associated with it";
        _type = type;
        _policy = policy;
        _longName = longName;
        _shortName = shortName;
        _shortHelp = shortHelp;
        _handler = handler;
    }

    public static Option newWithoutArgument(Policy policy,
                                            String longName, String shortName,
                                            String shortHelp, Handler handler)
    {
        return new Option(Void.class, policy, longName, shortName, shortHelp,
                          handler);
    }

    public static Option newStringOption(Policy policy,
                                         String longName, String shortName,
                                         String shortHelp, Handler handler)
    {
        return new Option(String.class, policy, longName, shortName, shortHelp,
                          handler);
    }

    public static Option newIntegerOption(Policy policy,
                                          String longName, String shortName,
                                          String shortHelp, Handler handler)
    {
        return new Option(Integer.class, policy, longName, shortName, shortHelp,
                          handler);
    }

    public static Option newHelpOption(Handler handler)
    {
        return Option.newWithoutArgument(Option.Policy.OPTIONAL,
                                         "help", "h",
                                         "show this help text",
                                         handler);
    }

    public ArgumentParser.Status setValue(String str)
        throws ArgumentParsingError
    {
        try {
            if (_type == Void.class) {
                if (!str.isEmpty()) {
                    throw new ArgumentParsingError(String.format(
                        "%s expects no argument - remove %s%nExample: %s",
                        name(), str, exampleUsageToString()));
                }
            } else if (_type == Integer.class) {
                _value = Integer.valueOf(str);
            } else if (_type == String.class) {
                if (str.isEmpty()) {
                    throw new ArgumentParsingError(String.format(
                        "%s expects an argument%nExample: %s",
                        name(), exampleUsageToString()));
                }
                _value = str;
            } else {
                throw new IllegalStateException(String.format(
                    "BUG: %s is of an unsupported type to %s%nExample: %s",
                    str, name(), exampleUsageToString()));
            }
            _numInstances++;
            if (_handler != null) {
                return _handler.handle(this);
            }
            return ArgumentParser.Status.CONTINUE;
        } catch (NumberFormatException e) {
            throw new ArgumentParsingError(String.format("%s - invalid value" +
                                                         " %s%n%s%nExample: %s",
                                                         name(),
                                                         str,
                                                         exampleUsageToString(),
                                                         e));
        }
    }

    public Object getValue()
    {
        if (!isSet()) {
            throw new IllegalStateException(String.format("%s has not been " +
                                                          "parsed yet", this));
        }
        return _value;
    }

    @Override
    public String toString()
    {
        return String.format("%s (type=%s policy=%s longName=%s shortName=%s)" +
                             " { value=%s }",
                             getClass().getSimpleName(), _type, _policy,
                             _longName, _shortName, _value);
    }

    public boolean isSet()
    {
        return _numInstances > 0;
    }

    public String name()
    {
        if (hasLongName()) {
            return String.format("--%s", _longName);
        } else {
            return String.format("-%s", _shortName);
        }
    }

    public boolean hasLongName()
    {
        return _longName.length() > 0;
    }

    public boolean hasShortName()
    {
        return _shortName.length() > 0;
    }

    public String shortHelp()
    {
        return _shortHelp;
    }

    public boolean expectsValue()
    {
        return _type != Void.class;
    }

    public String longName()
    {
        return _longName;
    }

    public String shortName()
    {
        return _shortName;
    }

    public boolean isRequired()
    {
        return _policy == Policy.REQUIRED;
    }

    public String exampleUsageToString()
    {
        String shortName = usageShortToString();
        String longName = usageLongToString();
        StringBuilder sb = new StringBuilder();
        if (shortName.length() > 0 && longName.length() > 0) {
            return sb.append(shortName).
                      append(" or ").
                      append(longName).toString();
        } else if (shortName.length() > 0) {
            return sb.append(shortName).toString();
        } else {
            return sb.append(longName).toString();
        }
    }

    public String usageShortToString()
    {
        if (_shortName.isEmpty()) {
            return "";
        } else if (expectsValue()) {
            return String.format("-%s <%s>", _shortName, _type.getSimpleName());
        } else {
            return String.format("-%s", _shortName);
        }
    }

    public String usageLongToString()
    {
        if (_longName.isEmpty()) {
            return "";
        } else if (expectsValue()) {
            return String.format("--%s=<%s>", _longName, _type.getSimpleName());
        } else {
            return String.format("--%s", _longName);
        }
    }
}