package com.hmdp.limter.exception;

public class RateLimitException extends RuntimeException
{   public RateLimitException(String message) {
    super(message);}
}
