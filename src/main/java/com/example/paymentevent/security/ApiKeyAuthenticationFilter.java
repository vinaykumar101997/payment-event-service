package com.example.paymentevent.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Authenticates a request whose X-API-Key header equals the configured key
 * (payments.api-key, from PAYMENTS_API_KEY). A missing or wrong key leaves the request
 * unauthenticated, and the security chain answers 401 (ApiKeySecurityConfig).
 *
 * The comparison is constant-time: both values are hashed with SHA-256 and the digests are
 * compared with MessageDigest.isEqual, which looks at every byte. Comparing the strings with
 * equals() would return as soon as a character differs, so response times would leak how
 * much of a guess is right. Hashing first also makes both inputs the same length, so the
 * key's length doesn't leak either.
 *
 * Not a @Component: Spring Boot would also register it as a plain servlet filter, running it
 * outside the security chain. ApiKeySecurityConfig creates it.
 */
public class ApiKeyAuthenticationFilter extends OncePerRequestFilter {

    public static final String HEADER = "X-API-Key";

    private final byte[] expectedDigest;

    public ApiKeyAuthenticationFilter(String expectedKey) {
        if (expectedKey == null || expectedKey.isBlank()) {
            throw new IllegalStateException("payments.api-key is empty: set the PAYMENTS_API_KEY environment variable");
        }
        this.expectedDigest = sha256(expectedKey);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        if (matches(request.getHeader(HEADER))) {
            SecurityContext context = SecurityContextHolder.createEmptyContext();
            context.setAuthentication(UsernamePasswordAuthenticationToken.authenticated(
                    "api-client", null, AuthorityUtils.createAuthorityList("ROLE_API_CLIENT")));
            SecurityContextHolder.setContext(context);
        }
        chain.doFilter(request, response);
    }

    boolean matches(String presentedKey) {
        if (presentedKey == null || presentedKey.isEmpty()) {
            return false;
        }
        return MessageDigest.isEqual(sha256(presentedKey), expectedDigest);
    }

    private static byte[] sha256(String value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by every Java platform", e);
        }
    }
}
