package com.example.paymentevent.repository;

import com.example.paymentevent.domain.Account;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface AccountRepository extends JpaRepository<Account, String> {

    /**
     * Takes a row-level SELECT ... FOR UPDATE lock. Callers must always lock the two
     * accounts involved in a payment in a consistent order (e.g. sorted by id) to avoid
     * deadlocking against a concurrent transfer running in the opposite direction.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT a FROM Account a WHERE a.id = :id")
    Optional<Account> findByIdForUpdate(@Param("id") String id);
}
