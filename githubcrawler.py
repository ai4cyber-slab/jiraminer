from concurrent.futures import ThreadPoolExecutor, wait
from typing import List
from github import Github
from github.GithubException import UnknownObjectException
from github.Repository import Repository
from jira_github_dataclasses import *
import requests
from pprint import pprint
import logging
from random import randint
from utils import *
import time
import re
import os
import json

COMMITSURL = "https://api.github.com/repos/{reponame}/pulls/{prnumber}/commits"
COMMITURL = "https://api.github.com/repos/{reponame}/commits/{sha}"
PRURL = "https://api.github.com/repos/{reponame}/pulls/{prnumber}"

TITLEREGEX = re.compile("[A-Z]+-[0-9]+")


class GithubCrawler:
    """Crawles repositories, pull requests, commits and files from github"""

    def __init__(self, auths=None, limit=4900):
        """Initializes a GithubCrawler

        Parameters
        ----------
        auths : List[Tuple], optional
            List of (username, secret) pairs to , by default None
        """
        self.auths = auths
        init_logger("githubcrawler", "log/crawler.log")
        self.logger = logging.getLogger("githubcrawler")
        self.downloaded_pullrequests = dict()
        self.downloaded_commits = dict()
        self.requestCounter = dict()
        self.ratelimits = dict()
        self.limit = limit
        for auth in auths:
            self.ratelimits[auth] = self.check_rate_limit(auth)

    def check_rate_limit(self, auth):
        """Updates the saved rate limit and request counter for a user

        Parameters
        ----------
        auth : _Tuple
            (username, secret) pair

        Returns
        -------
        Ratelimit
            Ratelimit object for the user
        """
        r = requests.get(
            "https://api.github.com/rate_limit",
            auth=auth,
        )
        ratelimit = to_dataclass(RateLimit, r.json()["resources"]["core"])
        print(ratelimit)
        self.ratelimits[auth] = ratelimit
        self.requestCounter[auth] = ratelimit.used

        return ratelimit

    def auth(self):
        """Randomly chooses a user to send the request with
           Counts the sent requests
           Sleeps until the rate limit reset, if the user is close to the limit
        Returns
        -------
        Tuple
            (username, secret) pair
        """
        auth = self.auths[randint(0, len(self.auths) - 1)]

        # checks the rate limit
        if self.requestCounter[auth] % 1000 < 1:
            self.check_rate_limit(auth)
        self.requestCounter[auth] += 1
        if self.requestCounter[auth] > self.limit:
            self.check_rate_limit(auth)
            # If the user is close to the limit, sleep until the limit resets
            sleeptime = self.ratelimits[auth].reset - time.time() + 60
            if self.requestCounter[auth] > 4950 and sleeptime > 0:
                self.logger.info(
                    "Waiting until %s.",
                    time.strftime(
                        "%Y-%m-%d %H:%M:%S",
                        time.localtime(self.ratelimits[auth].reset + 60),
                    ),
                )
                time.sleep(sleeptime)

        return auth

    def refresh_downloaded(self):
        """Clears the stored pull requests and commits"""
        self.downloaded_pullrequests.clear()
        self.downloaded_commits.clear()

    def download_commit(self, reponame: str, sha: str):
        """Downloads a commit's information from a repository
           If the commit has been downloaded before, then returns the stored version

        Parameters
        ----------
        reponame : str
            "owner/repositoryname"
        sha : str
            Commit hash

        Returns
        -------
        Commit
            The downloaded commit
        """

        # Commit already downloaded
        if sha in self.downloaded_commits:
            self.logger.info(
                "Commit (sha: %s) is already downloaded from repo %s.", sha, reponame
            )
            return self.downloaded_commits[sha]
        r = requests.get(
            COMMITURL.format(reponame=reponame, sha=sha),
            auth=self.auth(),
            params={"accept": "application/vnd.github+json"},
        )

        # If the resposne was not 200 OK, then an error happened
        if r.status_code != 200:
            self.logger.error(
                "Error happened while downloading commit, reponame: %s, sha: %s",
                reponame,
                sha,
            )
            return None
        else:
            self.logger.info(
                "Commit (sha: %s) downloaded successfully from repo %s.", sha, reponame
            )
        # Converts the downloaded commit information into a dataclass object
        commitdict = r.json()
        files = [to_dataclass(File, filedict) for filedict in commitdict["files"]]
        commit = Commit(
            sha=commitdict["sha"], message=commitdict["commit"]["message"], files=files
        )
        self.downloaded_commits[sha] = commit
        return commit

    def gather_commits_from_pull_requests(self, reponame: str, prnumber: int):
        """Gathers all the commits for a pull request

        Parameters
        ----------
        reponame : str
            "owner/repositoryname"
        prnumber : int
            Pull request number

        Returns
        -------
        List[Commit]
            List of commits
        """
        # Check if it has been already downloaded
        if (reponame, prnumber) in self.downloaded_pullrequests:
            self.logger.info(
                "Commits from %s repository %d pull request are already downloaded.",
                reponame,
                prnumber,
            )
            return self.downloaded_pullrequests[(reponame, prnumber)]

        per_page = 100
        page = 1
        commits = []
        total = 0
        done = False

        # Gathers all commits until less then the maximal page size is returned
        while not done:
            self.logger.info(
                "Downloading commits from %s repository %d pull request, params per_page: %d, page: %d.",
                reponame,
                prnumber,
                per_page,
                page,
            )
            params = {"per_page": per_page, "page": page}
            r = requests.get(
                COMMITSURL.format(reponame=reponame, prnumber=prnumber),
                params=params,
                auth=self.auth(),
            )
            # If the response is not 200 OK, log the error
            if r.status_code != 200:
                self.logger.error(
                    "Error happened with params per_page: %d, page: %d, status code %d.",
                    per_page,
                    page,
                    r.status_code,
                )
                return commits
            # Convert all commits into Commit objects
            for commitdict in r.json():
                commit = self.download_commit(reponame, commitdict["sha"])
                commit.pullrequest = prnumber
                commits.append(commit)
            page += 1
            commits_len = len(r.json())
            total += commits_len
            if commits_len < per_page:
                done = True
        return commits

    def gather_pulls(self, g: Github, owner, name, savefile):
        """Gathers all the pull requests for a repository and saves into a file

        Parameters
        ----------
        g : PyGithub.Github
            Github object with which to query
        owner : str
            Owner of the repository
        name : str
            Repository name
        savefile : str
            Name of the file to save the repository information
        """
        self.logger.info("Gathering pullrequests for repo %s.", owner + "/" + name)
        repo = try_except(
            lambda: g.get_user(owner).get_repo(name),
            lambda: self.logger.error("Repo %s unavailable", owner + "/" + name),
            UnknownObjectException,
        )
        if repo is None:
            return
        pulls = []
        # Gather all the pull requests
        for pull in repo.get_pulls(state="all"):
            issue_keys = None
            matches = re.findall(TITLEREGEX, pull.title)
            if len(matches) > 0:
                issue_keys = matches

            pulls.append(
                PullRequest(
                    pull.url,
                    pull.number,
                    pull.title,
                    issue_keys,
                )
            )
        repository = Repository(
            repo.full_name, repo.name, repo.description, repo.id, repo.url, pulls
        )
        # Write the repository into a file
        with open(savefile, "wt") as file:
            file.write(repository.to_json(indent=4))

    def gather_all_pulls(self, g: Github, repositories, saveDir):
        """Gather all pull requests for many repositories concurrently

        Parameters
        ----------
        g : PyGithub.Github
            Github object with which to query
        repositories : List[str]
            List of "owner/repositoryname" repositories
        saveDir : str
            A path to a directory where all the repositories information are saved
        """
        if not os.path.exists(saveDir):
            os.mkdir(saveDir)
        futures = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            for repository in repositories:
                try:
                    owner = repository.split("/")[0]
                    name = repository.split("/")[1]
                except:
                    self.logger.error("Error happened with %s repo", repository)
                    continue
                # If it is already downloaded and saved, then skip
                if os.path.exists(os.path.join(saveDir, name + ".json")):
                    self.logger.info("Repo %s skipped", repository)
                    continue
                futures.append(
                    executor.submit(
                        self.gather_pulls,
                        g,
                        owner,
                        name,
                        os.path.join(saveDir, name + ".json"),
                    )
                )
        for future in futures:
            if future.exception():
                print(future.result())
        wait(futures)


def download_repoinfo(g: Github, filename="githubrepos.json"):
    repos = []

    try:
        for repo in g.get_user("apache").get_repos():
            repos.append(
                Repository(
                    repo.full_name,
                    repo.name,
                    repo.description,
                    repo.id,
                    repo.url,
                )
            )
    except Exception as e:
        print(e)
    finally:
        repos = Repos(repos)
        with open(filename, "wt") as file:
            file.write(repos.to_json(indent=4))


def main():
    for auth in [
        ("xxx", "xxx"),
    ]:
        r = requests.get(
            "https://api.github.com/rate_limit",
            auth=auth,
        )
        ratelimit = to_dataclass(RateLimit, r.json()["resources"]["core"])
        print(
            auth,
            ratelimit,
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ratelimit.reset)),
        )


if __name__ == "__main__":
    main()
