package com.github.kevinarpe.scb.cache;

/**
 * @author Kevin Connor ARPE (kevinarpe@gmail.com)
 */
public enum IsFairLock {

    YES(true),
    NO(false),
    ;
    public static final IsFairLock JAVA_DEFAULT = NO;
    public final boolean booleanValue;

    private IsFairLock(boolean booleanValue) {
        this.booleanValue = booleanValue;
    }
}
