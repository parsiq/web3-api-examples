// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract VotingSystem {
    address public owner;
    mapping(uint256 => Candidate) public candidates;
    mapping(address => bool) public votes;
    uint256 public candidatesCount;
  

    struct Candidate {
        uint id;
        string name;
        uint voteCount;
    }

    event VoteEvent (
        uint indexed _candidateId
    );

    event NewCandidate (
        uint indexed _candidateId,
        string _name
    );

    constructor() {
        owner = msg.sender;
    }

    modifier onlyOwner() {
        require(msg.sender == owner);
        _;
    }

    function addCandidate(string memory _name) public onlyOwner {
        candidatesCount++;
        candidates[candidatesCount] = Candidate(candidatesCount, _name, 0);
        emit NewCandidate(candidatesCount, _name);
    }

    function vote(uint256 _candidateId) public {
        // Require that they haven't voted before
        require(!votes[msg.sender], "You have already voted.");

        // Require a valid candidate
        require(_candidateId > 0 && _candidateId <= candidatesCount, "Not a valid candidate.");

        // Record the voter has voted
        votes[msg.sender] = true;

        // Update candidate's vote count
        candidates[_candidateId].voteCount += 1;

        // Trigger voted event
        emit VoteEvent(_candidateId);
    }
}