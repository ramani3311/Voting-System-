package com.voting.controller;

import com.voting.config.VotingProperties;
import com.voting.dto.ResultsResponse;
import com.voting.dto.VoteRequest;
import com.voting.service.ResultsService;
import com.voting.service.VoteService;
import jakarta.validation.Valid;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/polls/{pollId}")
public class VoteController {

    private final VoteService votes;
    private final ResultsService results;
    private final VotingProperties props;

    public VoteController(VoteService votes, ResultsService results, VotingProperties props) {
        this.votes = votes;
        this.results = results;
        this.props = props;
    }

    @PostMapping("/votes")
    public ResponseEntity<Map<String, String>> vote(@PathVariable String pollId,
                                                    @Valid @RequestBody VoteRequest req) {
        VotingProperties.Poll poll = poll(pollId);
        if (!poll.options().contains(req.optionId())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unknown option: " + req.optionId());
        }
        VoteService.Result r = votes.cast(pollId, req.userId(), req.optionId());
        if (r.outcome() == VoteService.Outcome.DUPLICATE) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "You have already voted in this poll");
        }
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of("status", "ACCEPTED", "eventId", r.eventId()));
    }

    @GetMapping("/results")
    public ResultsResponse results(@PathVariable String pollId) {
        return results.results(poll(pollId));
    }

    private VotingProperties.Poll poll(String pollId) {
        return props.find(pollId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Poll not found"));
    }
}
