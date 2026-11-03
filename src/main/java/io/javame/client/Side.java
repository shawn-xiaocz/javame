package io.javame.client;

/** Limit-order direction. */
public enum Side {
    BUY, SELL;

    public Side opposite() {
        return this == BUY ? SELL : BUY;
    }
}
