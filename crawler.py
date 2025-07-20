import requests
import re
import logging
from jira_github_dataclasses import *
import os
from pathlib import Path
from utils import *

from githubcrawler import COMMITURL, GithubCrawler, Repos, Commit

BASEURL = "https://issues.apache.org/jira/rest/api/2/"
SEARCHURL = "https://issues.apache.org/jira/rest/api/2/search"

GITHUBREGEX = re.compile(
    "((http(s)?:\/\/)?github.com[-a-zA-Z0-9()@:%_\+.~?&//=]*\/[-a-zA-Z0-9()@:%_\+.~?&//=]*)"
)

APIGITHUBREGEX = re.compile(
    "((http(s)?:\/\/)?api.github.com/repos/(?P<reponame>[-a-zA-Z0-9()@:%_\+.~?&//=])*)"
)

PULLREQUESTORCOMMITREGEX = re.compile(
    "(https?:\/\/)?github.com\/(?P<reponame>[-a-zA-Z0-9()@:%_\+.~?&//=]*)\/((pull\/(?P<prnumber>[0-9]*))|(commit\/(?P<commithash>[a-zA-Z0-9]*)))"
)

PULLREQUESTREGEX = re.compile(
    "(?P<url>(https?:\/\/)?github.com\/(?P<reponame>[-a-zA-Z0-9()@:%_\+.~?&//=]*)\/pull\/(?P<prnumber>[0-9]*))"
)

COMMITREGEX = re.compile(
    "(?P<url>(https?:\/\/)?github.com\/(?P<reponame>[-a-zA-Z0-9()@:%_\+.~?&//=]*)\/commit\/(?P<commithash>[a-zA-Z0-9]*))"
)

RAWREGEX = re.compile(
    "(https?:\/\/)?github.com\/(?P<reponame>[-a-zA-Z0-9()@:%_\+.~?&//=]*)\/raw"
)


class Crawler:
    def __init__(self):
        init_logger("crawler")
        self.logger = logging.getLogger("crawler")
        self.github = GithubCrawler(
            [
                ("meszarosp", "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"),
                ("amilankovich-slab", "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"),
                ("searchlab-team", "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"),
            ]
        )

    def gather_worklog_for_issue(self, issue: Issue):
        r = requests.get(issue.self + "/worklog")
        return str(r.json())

    @log_time(loggerName="crawler")
    def get_issues(
        self, project: str, total=None, maxResults=1000, startAt=0
    ) -> Issues:
        session = requests.Session()
        if total == None:
            params = {
                "jql": f'project="{project}"',
                "maxResults": 0,
                "startAt": 0,
            }
            r = session.get(SEARCHURL, params=params)

        if r.status_code != 200:
            message = f"{project} not found"
            raise Exception(message)

        total = r.json()["total"]
        self.logger.info(
            "Downloading issues from %s project. total: %d, maxResults: %d, startAt: %d",
            project,
            total,
            maxResults,
            startAt,
        )
        params = {
            "jql": f'project="{project}"',
            "maxResults": maxResults,
            "startAt": startAt,
            "fields": "description,comment,status,issuetype, created, updated, resolved",
        }
        issues = []
        while startAt < total:
            r = session.get(SEARCHURL, params=params)
            self.logger.info(
                "Downloading issues from %s project, startAt: %d, %.2f%% downloaded.",
                project,
                startAt,
                float(100 * startAt / total),
            )
            jsondict = r.json()
            for issue in jsondict["issues"]:
                issue_object = to_dataclass(Issue, issue)
                worklogs = self.gather_worklog_for_issue(issue_object)
                issue_object.commitlinks = set()
                issue_object.prlinks = set()
                for match in re.finditer(PULLREQUESTORCOMMITREGEX, str(issue)):
                    if match.groupdict()["commithash"] == None:
                        issue_object.prlinks.add(match.group(0))
                    else:
                        issue_object.commitlinks.add(match.group(0))

                for match in re.finditer(PULLREQUESTORCOMMITREGEX, worklogs):
                    if match.groupdict()["commithash"] == None:
                        issue_object.prlinks.add(match.group(0))
                    else:
                        issue_object.commitlinks.add(match.group(0))

                issue_object.commitlinks = list(issue_object.commitlinks)
                issue_object.prlinks = list(issue_object.prlinks)

                issues.append(issue_object)
            startAt += maxResults
            params["startAt"] = startAt

        self.logger.info("100%% of issues from %s project downloaded.", project)
        return Issues(issues)

    def get_and_save_issues(
        self, project: str, total=None, maxResults=1000, startAt=0, filename: str = None
    ):
        if filename == None:
            filename = project.lower() + ".json"
        with open(filename, "wt") as file:
            file.write(
                self.get_issues(
                    project=project, total=total, maxResults=maxResults, startAt=startAt
                ).to_json(indent=4)
            )

    @log_time(loggerName="crawler")
    def gather_commits_for_project(self, project: Project, tempfile: str):
        self.logger.info("Gathering commits for %s project", project.self)
        with open(tempfile, "at") as file:
            for issue in project.issues:
                self.gather_commits_for_issue(issue)
                file.write(issue.to_json(indent=4))
                file.write(",\n")
        self.logger.info("Total used requests: %s", str(self.github.requestCounter))

    @log_time(loggerName="crawler")
    def gather_commits_for_issue(self, issue: Issue):
        self.logger.info(
            "Gathering commits for %s issue from pull requests.", issue.self
        )
        issue.commits = []
        for pr in issue.prlinks:
            try:
                match = re.match(PULLREQUESTREGEX, pr)
                match = match.groupdict()
                reponame = match["reponame"]
                prnumber = int(match["prnumber"])
                issue.commits.extend(
                    self.github.gather_commits_from_pull_requests(reponame, prnumber)
                )
            except Exception as e:
                self.logger.error(
                    "Error (%s) happened while downloading %s from %s issue.",
                    str(e),
                    pr,
                    issue.self,
                )

        if len(issue.commitlinks) != 0:
            self.logger.info(
                "Gathering individual commits for %s issue from commit links.",
                issue.self,
            )
        for commitlink in issue.commitlinks:
            try:
                match = re.match(COMMITREGEX, commitlink)
                match = match.groupdict()
                reponame = match["reponame"]
                sha = match["commithash"]
                issue.commits.append(self.github.download_commit(reponame, sha))
            except Exception as e:
                self.logger.error(
                    "Error (%s) happened while downloading %s from %s issue.",
                    str(e),
                    commitlink,
                    issue.self,
                )

    @log_time("crawler")
    def download_files_for_commit(self, commit: Commit, rootDirectory: str):
        if len(commit.files) == 0:
            return
        reponame = re.match(RAWREGEX, commit.files[0].raw_url).groupdict()["reponame"]
        session = requests.Session()
        self.logger.info(
            "Downloading files from %s repo %s commit.", reponame, commit.sha
        )
        for file in commit.files:
            if Path(file.filename).suffix != ".java":
                continue
            if commit.pullrequest is not None:
                dirpath = os.path.join(
                    rootDirectory, reponame, str(commit.pullrequest), commit.sha
                )
            else:
                dirpath = os.path.join(rootDirectory, reponame, commit.sha)
            path = os.path.join(
                dirpath,
                file.filename.replace("/", "_"),
            )
            r = session.get(file.raw_url)
            Path(dirpath).mkdir(parents=True, exist_ok=True)
            open(path, "wb").write(r.content)
            commit.path = path

    @log_time("crawler")
    def download_files_for_project(self, project: Project, rootDirectory: str):
        self.logger.info(
            "Downloading files from %s %s project.", project.key, project.self
        )
        for issue in project.issues:
            for commit in issue.commits:
                self.download_files_for_commit(
                    commit, os.path.join(rootDirectory, project.key)
                )

    def find_repo(self, projects: Projects, repos: Repos):
        repos = repos.repos
        for project in projects.projects:
            repo = next(
                (
                    repository
                    for repository in repos
                    if project.key.lower() in repository.full_name.lower()
                    or project.name.lower() in repository.full_name.lower()
                    or (
                        hasattr(repository.description, "lower")
                        and (
                            project.key.lower() in repository.description.lower()
                            or project.name.lower() in repository.description.lower()
                        )
                    )
                ),
                None,
            )
            if repo is not None:
                project.repo_url = repo.url
            else:
                print(project.name)


def filter_links_in_projecty_by_reponame(project: Project):
    if project.repo_url != None:
        norepo = False
        modified = False
        project_reponame = re.match(APIGITHUBREGEX, project.repo_url).groupdict()[
            "reponame"
        ]

        for issue in project.issues:
            newlinks = []
            for prlink in issue.prlinks:
                prlink_reponame = re.match(PULLREQUESTREGEX, prlink).groupdict()[
                    "reponame"
                ]
                if prlink_reponame == project_reponame:
                    newlinks.append(prlink)
            modified = True if len(newlinks) < len(issue.prlinks) else modified
            issue.prlinks = newlinks

            newlinks = []
            for commitlink in issue.commitlinks:
                commitlink_reponame = re.match(COMMITREGEX, commitlink).groupdict()[
                    "reponame"
                ]
                if commitlink_reponame == project_reponame:
                    newlinks.append(commitlink)
            modified = True if len(newlinks) < len(issue.commitlinks) else modified
            issue.commitlinks = newlinks
    else:
        norepo = True
        modified = False
    return norepo, modified


def no_links(project: Project):
    return all(
        len(issue.prlinks) == 0 and len(issue.commitlinks) == 0
        for issue in project.issues
    )


def download_all_files(projectsDir: str):
    crawler = Crawler()

    for filename in os.listdir(projectsDir):
        fullpath = os.path.join(projectsDir, filename)
        print(filename)
        with open(fullpath, "rt") as file:
            project = Project.from_json(file.read())
        crawler.download_files_for_project(project, "commits")
        # crawler.gather_commits_for_project(project, os.path.join("tempfiles", filename))
        fullpath = os.path.join(projectsDir, filename)
        with open(fullpath, "wt") as file:
            file.write(project.to_json(indent=4))


def download_commits(projectsDir: str, saveDir: str):
    crawler = Crawler()

    for filename in os.listdir(projectsDir):
        fullpath = os.path.join(projectsDir, filename)
        print(filename)
        with open(fullpath, "rt") as file:
            project = Project.from_json(file.read())
        # crawler.download_files_for_project(project, "commits")
        crawler.gather_commits_for_project(project, os.path.join("tempfiles", filename))
        fullpath = os.path.join(saveDir, filename)
        with open(fullpath, "wt") as file:
            file.write(project.to_json(indent=4))


@log_time(loggerName="crawler")
def main():
    # download_commits("projects", "projects_all_with_commits")

    crawler = Crawler()

    # projectsDir = "projects"
    # for filename in os.listdir(projectsDir):
    #     fullpath = os.path.join(projectsDir, filename)
    #     print(filename)
    #     with open(fullpath, "rt") as file:
    #         project = Project.from_json(file.read())
    #     for issue in project.issues:
    #         newlinks = []
    #         for prlink in issue.prlinks:
    #             newlinks.append(re.match(PULLREQUESTREGEX, prlink).groupdict()["url"])
    #         if len(issue.prlinks) < len(newlinks):
    #             print(project.key, issue.self)
    #         issue.prlinks = newlinks

    #         newlinks = []
    #         for clink in issue.commitlinks:
    #             newlinks.append(re.match(COMMITREGEX, clink).groupdict()["url"])
    #         if len(issue.commitlinks) < len(newlinks):
    #             print(project.key, issue.self)
    #         issue.commitlinks = newlinks
    #     fullpath = os.path.join(projectsDir, filename)
    #     with open(fullpath, "wt") as file:
    #         file.write(project.to_json(indent=4))


if __name__ == "__main__":
    main()
