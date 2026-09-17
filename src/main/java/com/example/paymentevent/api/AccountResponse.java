package com.example.paymentevent.api;

import com.example.paymentevent.domain.Account;

import java.math.BigDecimal;

public record AccountResponse(String id, BigDecimal balance, String currency) {

    public static AccountResponse from(Account account) {
        return new AccountResponse(account.getId(), account.getBalance(), account.getCurrency());
    }
}
