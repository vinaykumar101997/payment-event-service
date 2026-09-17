package com.example.paymentevent.api;

import com.example.paymentevent.domain.Account;
import com.example.paymentevent.exception.ResourceNotFoundException;
import com.example.paymentevent.repository.AccountRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/accounts")
public class AccountController {

    private final AccountRepository accountRepository;

    public AccountController(AccountRepository accountRepository) {
        this.accountRepository = accountRepository;
    }

    @GetMapping("/{id}")
    public AccountResponse getAccount(@PathVariable String id) {
        Account account = accountRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Account not found: " + id));
        return AccountResponse.from(account);
    }
}
