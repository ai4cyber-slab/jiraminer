from typing import List, Optional
from dataclasses_json import dataclass_json
from dataclasses import dataclass, field

# JIRA dataclasses
# https://docs.atlassian.com/software/jira/docs/api/REST/7.6.1/#api/2/issuetype
@dataclass_json
@dataclass
class IssueType:
    self: str
    # id: int
    # description: str
    name: str


# https://docs.atlassian.com/software/jira/docs/api/REST/7.6.1/#api/2/status
@dataclass_json
@dataclass
class Status:
    self: str
    # description: str
    name: str
    # id: int


@dataclass_json
@dataclass
class File:
    sha: str
    filename: str
    status: str
    contents_url: str
    raw_url: str = None


@dataclass_json
@dataclass
class Commit:
    sha: str
    message: str
    files: List[File] = None
    pullrequest: int = None
    path: str = None


# https://docs.atlassian.com/software/jira/docs/api/REST/7.6.1/#api/2/issue
@dataclass_json
@dataclass
class Issue:
    self: str
    id: int
    key: str
    issuetype: IssueType = None
    description: str = None
    status: Status = None
    created: str = None
    updated: str = None
    resolved: str = None
    prlinks: list[str] = None
    commitlinks: list[str] = None
    commits: list[Commit] = None


# Github dataclasses


@dataclass_json
@dataclass
class PullRequest:
    url: str
    number: int
    title: str = None
    created_at = None
    updated_at = None
    closed_at = None
    merged_at = None
    issue_key: List[str] = None
    issue: List[Issue] = None
    commits: list[Commit] = None


@dataclass_json
@dataclass
class Repository:
    full_name: str
    name: str
    description: str
    id: int
    url: str
    pulls: List[PullRequest]


@dataclass_json
@dataclass
class Repos:
    repos: List[Repository]


@dataclass_json
@dataclass
class RateLimit:
    limit: int
    remaining: int
    reset: int
    used: int


@dataclass_json
@dataclass
class Issues:
    issues: List[Issue]

    def __getitem__(self, item):
        return self.issues[item]

    def __len__(self):
        return len(self.issues)


# https://docs.atlassian.com/software/jira/docs/api/REST/7.6.1/#api/2/project
@dataclass_json
@dataclass
class Project:
    self: str
    id: int
    key: str
    name: str
    url: str = None
    repo_url: str = None
    issues: list[Issue] = field(default_factory=lambda: [])
    reponames: list[str] = field(default_factory=lambda: [])


@dataclass_json
@dataclass
class Projects:
    projects: List[Project]
