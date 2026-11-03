package io.javame.client;

/** Persistent order state; cancelled orders retain their unfilled quantity for audit. */
public enum OrderStatus {
    PENDING, OPEN, PARTIALLY_FILLED, FILLED, CANCELLED
}
