package com.voting.dto;

import java.util.List;

public record ResultsResponse(String pollId, String title, long total, List<Count> counts) {
    public record Count(String optionId, long votes) {}
}
