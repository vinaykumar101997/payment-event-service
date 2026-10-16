package com.example.paymentevent.security;

import jakarta.servlet.DispatcherType;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.AnonymousAuthenticationFilter;
import org.springframework.web.servlet.HandlerExceptionResolver;

/**
 * API key authentication for the whole API. Everything needs a valid X-API-Key except
 * /actuator/health (left open for liveness checks) and the servlet ERROR dispatch (so an
 * error on an authenticated request can still be rendered). Deny by default: a new endpoint
 * is protected without anyone remembering to list it.
 *
 * Stateless: no session, no cookies, so CSRF protection (which defends cookie-based
 * sessions) is off, as are form login, HTTP Basic and logout.
 *
 * A missing or wrong key goes to the entry point, which hands the AuthenticationException to
 * Spring MVC's exception resolvers, so ApiExceptionHandler renders it as the same
 * application/problem+json as every other error (401 "unauthorized").
 */
@Configuration
public class ApiKeySecurityConfig {

    @Bean
    public SecurityFilterChain apiKeySecurityFilterChain(HttpSecurity http,
                                                         @Value("${payments.api-key}") String apiKey,
                                                         AuthenticationEntryPoint problemEntryPoint) throws Exception {
        http
                .csrf(AbstractHttpConfigurer::disable)
                .formLogin(AbstractHttpConfigurer::disable)
                .httpBasic(AbstractHttpConfigurer::disable)
                .logout(AbstractHttpConfigurer::disable)
                .requestCache(AbstractHttpConfigurer::disable)
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        .dispatcherTypeMatchers(DispatcherType.ERROR).permitAll()
                        .requestMatchers("/actuator/health", "/actuator/health/**").permitAll()
                        .anyRequest().authenticated())
                .addFilterBefore(new ApiKeyAuthenticationFilter(apiKey), AnonymousAuthenticationFilter.class)
                .exceptionHandling(exceptions -> exceptions.authenticationEntryPoint(problemEntryPoint));
        return http.build();
    }

    @Bean
    public AuthenticationEntryPoint problemEntryPoint(
            @Qualifier("handlerExceptionResolver") HandlerExceptionResolver handlerExceptionResolver) {
        return (request, response, authException) ->
                handlerExceptionResolver.resolveException(request, response, null, authException);
    }
}
