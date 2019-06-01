package com.modelicious.in.logging;
import org.apache.log4j.Level;
 
/**
 * http://crunchify.com/java-how-to-create-your-own-logging-level-in-log4j-configuring-log4j/
 * 
 */
 
@SuppressWarnings("serial")
public class FileLog4jLevel extends Level {
 
    /**
     * Value of FileLog4jLevel level. This value is lesser than DEBUG_INT and higher
     * than TRACE_INT}
     */
    public static final int FILE_INT = ERROR_INT + 10;
 
    /**
     * Level representing my log level
     */
    public static final Level FILE = new FileLog4jLevel(FILE_INT, "FILE", 10);
 
    /**
     * Constructor
     */
    protected FileLog4jLevel(int arg0, String arg1, int arg2) {
        super(arg0, arg1, arg2);
 
    }
 
    /**
     * Checks whether logArgument is "FILE" level. If yes then returns
     * FILE}, else calls FileLog4jLevel#toLevel(String, Level) passing
     * it Level#DEBUG as the defaultLevel.
     */
    public static Level toLevel(String logArgument) {
        if (logArgument != null && logArgument.toUpperCase().equals("FILE")) {
            return FILE;
        }
        return (Level) toLevel(logArgument);
    }
 
    /**
     * Checks whether val is FileLog4jLevel#FILE_INT. If yes then
     * returns FileLog4jLevel#FILE, else calls
     * FileLog4jLevel#toLevel(int, Level) passing it Level#DEBUG as the
     * defaultLevel
     * 
     */
    public static Level toLevel(int val) {
        if (val == FILE_INT) {
            return FILE;
        }
        return (Level) toLevel(val, Level.DEBUG);
    }
 
    /**
     * Checks whether val is FileLog4jLevel#FILE_INT. If yes
     * then returns FileLog4jLevel#FILE, else calls Level#toLevel(int, org.apache.log4j.Level)
     * 
     */
    public static Level toLevel(int val, Level defaultLevel) {
        if (val == FILE_INT) {
            return FILE;
        }
        return Level.toLevel(val, defaultLevel);
    }
 
    /**
     * Checks whether logArgument is "FILE" level. If yes then returns
     * FileLog4jLevel#FILE, else calls
     * Level#toLevel(java.lang.String, org.apache.log4j.Level)
     * 
     */
    public static Level toLevel(String logArgument, Level defaultLevel) {
        if (logArgument != null && logArgument.toUpperCase().equals("FILE")) {
            return FILE;
        }
        return Level.toLevel(logArgument, defaultLevel);
    }
}