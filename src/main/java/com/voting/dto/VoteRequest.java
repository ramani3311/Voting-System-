package com.voting.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record VoteRequest(
        @NotBlank @Size(max = 64) String userId,
        @NotBlank @Size(max = 64) String optionId) {}
