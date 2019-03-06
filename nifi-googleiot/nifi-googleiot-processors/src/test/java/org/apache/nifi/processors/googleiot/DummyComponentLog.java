package org.apache.nifi.processors.googleiot;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;

public class DummyComponentLog implements ComponentLog {

    @Override
    public void warn(String s, Throwable throwable) {
        System.out.println(s);
    }

    @Override
    public void warn(String s, Object[] objects) {
        System.out.println(s);
    }

    @Override
    public void warn(String s, Object[] objects, Throwable throwable) {
        System.out.println(s);
    }

    @Override
    public void warn(String s) {
        System.out.println(s);
    }

    @Override
    public void trace(String s, Throwable throwable) {
        System.out.println(s);
    }

    @Override
    public void trace(String s, Object[] objects) {
        System.out.println(s);
    }

    @Override
    public void trace(String s) {
        System.out.println(s);
    }

    @Override
    public void trace(String s, Object[] objects, Throwable throwable) {
        System.out.println(s);
    }

    @Override
    public boolean isWarnEnabled() {
        return false;
    }

    @Override
    public boolean isTraceEnabled() {
        return false;
    }

    @Override
    public boolean isInfoEnabled() {
        return false;
    }

    @Override
    public boolean isErrorEnabled() {
        return false;
    }

    @Override
    public boolean isDebugEnabled() {
        return false;
    }

    @Override
    public void info(String s, Throwable throwable) {
        System.out.println(s);
    }

    @Override
    public void info(String s, Object[] objects) {
        System.out.println(s);
    }

    @Override
    public void info(String s) {
        System.out.println(s);
    }

    @Override
    public void info(String s, Object[] objects, Throwable throwable) {
        System.out.println(s);
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void error(String s, Throwable throwable) {
        System.out.println(s);
    }

    @Override
    public void error(String s, Object[] objects) {
        System.out.println(s);
    }

    @Override
    public void error(String s) {
        System.out.println(s);
    }

    @Override
    public void error(String s, Object[] objects, Throwable throwable) {
        System.out.println(s);
    }

    @Override
    public void debug(String s, Throwable throwable) {
        System.out.println(s);
    }

    @Override
    public void debug(String s, Object[] objects) {
        System.out.println(s);
    }

    @Override
    public void debug(String s, Object[] objects, Throwable throwable) {
        System.out.println(s);
    }

    @Override
    public void debug(String s) {
        System.out.println(s);
    }

    @Override
    public void log(LogLevel logLevel, String s, Throwable throwable) {
        System.out.println(s);
    }

    @Override
    public void log(LogLevel logLevel, String s, Object[] objects) {
        System.out.println(s);
    }

    @Override
    public void log(LogLevel logLevel, String s) {
        System.out.println(s);
    }

    @Override
    public void log(LogLevel logLevel, String s, Object[] objects, Throwable throwable) {
        System.out.println(s);
    }
}
