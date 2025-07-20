from cmath import e
from concurrent.futures import ThreadPoolExecutor, wait
import logging
from typing import Dict
import requests
import re
import os
from pathlib import Path
from githubcrawler import GithubCrawler
from jira_github_dataclasses import *
from utils import *

import jsonargparse

BASEURL = "https://issues.apache.org/jira/rest/api/2/"
SEARCHURL = "https://issues.apache.org/jira/rest/api/2/search"

GITHUBREGEX = re.compile(
    "((http(s)?:\/\/)?github.com[-a-zA-Z0-9()@:%_\+.~?&//=]*\/[-a-zA-Z0-9()@:%_\+.~?&//=]*)"
)

APIGITHUBREGEX = re.compile(
    "((http(s)?:\/\/)?api.github.com/repos/(?P<reponame>[-a-zA-Z0-9()@:%_\+.~?&//=])*)"
)

PULLREQUESTORCOMMITREGEX = re.compile(
    "(https?:\/\/)?github.com\/(?P<reponame>[-a-zA-Z0-9()@_\+.~?&//=]*)\/((pull\/(?P<prnumber>[0-9]*))|(commit\/(?P<commithash>[a-zA-Z0-9]*)))"
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
    def __init__(
        self,
        loggerName: str = "crawler",
        logFileName: str = "log/crawler.log",
        auth=None,
    ):
        init_logger(loggerName, logFileName)
        self.logger = logging.getLogger(loggerName)
        self.github = GithubCrawler(
            [
                ("meszarosp", "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"),
                ("amilankovich-slab", "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"),
                ("searchlab-team", "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"),
            ]
        )
        self.auth = auth

    def url_retrieve(self, url: str, filename: str):
        """Streams and downloads a file from a url

        Parameters
        ----------
        url : str
            Url to the file
        filename : str
            Path where the file is saved
        """
        r = requests.get(url, stream=True)
        if r.status_code != 200:
            self.logger.error("URL %s could not be retrieved.", url)
        with open(filename, "wb") as file:
            for chunk in r.iter_content():
                file.write(chunk)

    @log_time(loggerName="crawler")
    @handle_error(loggerName="crawler")
    def download_files_for_commit(
        self, commit: Commit, rootDirectory: str, permittedSuffixes=[".java"]
    ):
        """For a given commit download all files with a permitted suffix

        Parameters
        ----------
        commit : Commit
            Commit object
        rootDirectory : str
            A root directory where all the files are saved
        permittedSuffixes : list, optional
            A list of suffixes, which signals that the file should be downloaded, by default [".java"]
        """
        if len(commit.files) == 0:
            return
        reponame = re.match(RAWREGEX, commit.files[0].raw_url).groupdict()["reponame"]
        self.logger.info(
            "Downloading files from %s repo %s commit.", reponame, commit.sha
        )
        for file in commit.files:
            # Check if the suffix is correct
            if Path(file.filename).suffix not in permittedSuffixes:
                continue
            # If the commit belongs to a pull request, then the path will contain the pull request number
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
            Path(dirpath).mkdir(parents=True, exist_ok=True)
            if not Path(path).exists():
                self.url_retrieve(file.raw_url, path)
        commit.path = os.path.join(rootDirectory, reponame, commit.sha)

    @log_time(loggerName="crawler")
    def download_files_for_project(self, project: Project, rootDirectory: str):
        """Downloads all files for all the commints in a repository concurrently

        Parameters
        ----------
        project : Project
            Project object
        rootDirectory : str
            A root directory where all the files are saved into a directory with the project's key's name
        """
        self.logger.info(
            "Downloading files from %s %s project.", project.key, project.self
        )
        futures = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            for issue in project.issues:
                for commit in issue.commits:
                    futures.append(
                        executor.submit(
                            self.download_files_for_commit,
                            commit,
                            os.path.join(rootDirectory, project.key),
                        )
                    )
        wait(futures)

    def download_files_for_repository(
        self, repo: Repository, rootDirectory: str, onlyRelevant=True
    ):
        """Downloads all files for the repository concurrently

        Parameters
        ----------
        repo : Repository
            Repository object containing pull requests with their issues
        rootDirectory : str
            A root directory where all the files are saved into a directory with the repository's name
        onlyRelevant : bool, optional
            To download only those pull requests, which have an associated issue, by default True
        """
        self.logger.info("Downloading files from %s repository.", repo.full_name)
        futures = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            for pull in repo.pulls:
                if onlyRelevant and not pull.issue:
                    continue
                pull.commits = self.github.gather_commits_from_pull_requests(
                    repo.full_name, pull.number
                )
                for commit in pull.commits:
                    futures.append(
                        executor.submit(
                            self.download_files_for_commit,
                            commit,
                            rootDirectory,
                        )
                    )
        wait(futures)

    def download_files_for_repositories(
        self, repositoriesDir: str, rootDirectory: str, saveDir: str, onlyRelevant=True
    ):
        """Downloads all files for all the commits in a repository concurrently

        Parameters
        ----------
        repositoriesDir : str
            _description_
        rootDirectory : str
            A root directory where all the files are saved into a directory with the repository's name
        saveDir : str
            Path to a directory where the updated repositories are saved with the path to the commits
        onlyRelevant : bool, optional
            To download only those pull requests, which have an associated issue, by default True
        """
        os.makedirs(saveDir, exist_ok=True)
        for filename in os.listdir(repositoriesDir):
            savePath = os.path.join(saveDir, filename)
            fullpath = os.path.join(repositoriesDir, filename)
            with open(fullpath, "rt") as file:
                repo = Repository.from_json(file.read())
            self.download_files_for_repository(repo, rootDirectory)
            with open(savePath, "wt") as file:
                file.write(repo.to_json(indent=4))

    def download_all_files(self, projectsDir: str, rootDirectory: str, saveDir: str):
        """Scans a directory with projects saved into json-s and downloads all files for them

        Parameters
        ----------
        projectsDir : str
            Path to a directory which contains projects
        rootDirectory : str
            A root directory where all the files are saved for all projects
        saveDir : str
            Path to a directory where the updated projects are saved with the path to the commits
        """
        for filename in os.listdir(projectsDir):
            savePath = os.path.join(saveDir, filename)
            fullpath = os.path.join(projectsDir, filename)
            with open(fullpath, "rt") as file:
                project = Project.from_json(file.read())
            if not os.path.exists(os.path.join(rootDirectory, project.key)):
                self.download_files_for_project(project, rootDirectory)
                with open(savePath, "wt") as file:
                    file.write(project.to_json(indent=4))

    def filterfiles(self, saveDir: str) -> Dict:
        """Filters the files in the save directory
            If there are multiple saves of the same file with "_{number}" prefixes,
            then only the last one is kept and continued
            If there is a version without the prefix, then only that one is kept

        Parameters
        ----------
        saveDir : str
            Save directory

        Returns
        -------
        Dict
            Dictionary of projectname-filename pairs
        """
        filenames = dict()
        for filename in os.listdir(saveDir):
            try:
                projectname, number = re.split("\.|_", filename)
            except ValueError:
                filenames[filename.split("_")[0]] = filename
                continue
            if projectname in filenames:
                split = re.split("\.|_", filenames[projectname])
                if len(split) == 3:
                    if int(number) > int(split[1]):
                        filenames[projectname] = filename
            else:
                filenames[projectname] = filename

        return filenames

    def download_commits(self, projectsDir: str, saveDir: str):
        """Gathers all the commits for the projects and saves them into another directory

        Parameters
        ----------
        projectsDir : str
            Path to a directory, where the projects are in json format
        saveDir : str
            Path to a directory, where the projects with the commits will be saved in json format
        """
        if not os.path.exists(saveDir):
            os.mkdir(saveDir)
        filenames = self.filterfiles(saveDir)
        for filename in os.listdir(projectsDir):
            fullpath = os.path.join(projectsDir, filename)
            with open(fullpath, "rt") as file:
                project = Project.from_json(file.read())
            savedProjectPath = filenames.get(
                project.key, os.path.join(saveDir, filename)
            )
            self.gather_commits_for_project(
                project,
                os.path.join(saveDir, filename),
                os.path.join(saveDir, savedProjectPath),
            )
            fullpath = os.path.join(saveDir, filename)
            with open(fullpath, "wt") as file:
                file.write(project.to_json(indent=4))
            for old_filename in filter(
                lambda x: x.startswith(filename.split(".")[0] + "_"),
                os.listdir(saveDir),
            ):
                os.remove(os.path.join(saveDir, old_filename))

    def gather_worklog_for_issue(self, issue: Issue, issue_dict: Dict):
        """Gathers all the worklogs for an issue

        Parameters
        ----------
        issue : Issue
            issue object
        issue_dict : bool
            Issue dictionary from http response

        Returns
        -------
        str
            Worklog as a string
        """
        result = ""
        try:
            if int(issue_dict["fields"]["worklog"]["maxResults"]) < int(
                issue_dict["fields"]["worklog"]["total"]
            ):
                r = requests.get(issue.self + "/worklog", auth=self.auth)
                result = str(r.json())

        finally:
            return result

    def gather_comments_for_issue(self, issue: Issue, issue_dict: Dict):
        """Gathers all the comments for an issue

        Parameters
        ----------
        issue : Issue
            issue object
        issue_dict : bool
            Issue dictionary from http response

        Returns
        -------
        str
            Comments as a string
        """
        result = ""
        try:
            if int(issue_dict["fields"]["comment"]["maxResults"]) < int(
                issue_dict["fields"]["comment"]["total"]
            ):
                result = []
                startAt = 0
                found = False
                # Go through all pages until it is empty
                while not found:
                    params = {
                        "startAt": startAt,
                    }
                    r = requests.get(
                        issue.self + "/comment", params=params, auth=self.auth
                    )
                    if r.json()["comments"] == []:
                        found = True
                    result.append(str(r.json()))
                    startAt += 50

                result = " ".join(result)

        finally:
            return result

    @handle_error(loggerName="crawler")
    def gather_links_for_issue(self, issue: Issue, issue_dict: Dict):
        """Gathers all the commit and pull request links for an issue

        Parameters
        ----------
        issue : Issue
            issue object
        issue_dict : bool
            Issue dictionary from http response
        """
        # Use set to get all links once
        issue.commitlinks = set()
        issue.prlinks = set()
        # Get worklogs and comments
        worklogs = self.gather_worklog_for_issue(issue, issue_dict)
        comments = self.gather_comments_for_issue(issue, issue_dict)

        for index, elem in enumerate([str(issue_dict), worklogs, comments]):
            # try:
            # Use regex to match
            for match in re.finditer(PULLREQUESTORCOMMITREGEX, elem):
                if match.groupdict()["commithash"] == None:
                    issue.prlinks.add(match.group(0))
                else:
                    issue.commitlinks.add(match.group(0))
        # except:
        # self.logger.error("Error happaned in %s %d.", issue.self, index)

        issue.commitlinks = list(issue.commitlinks)
        issue.prlinks = list(issue.prlinks)

    def iterate_through_issues(
        self, project: str, total=None, maxResults=1000, startAt=0
    ):
        """Iterates through all issues in a project from Apache's JIRA repository

        Parameters
        ----------
        project : str
            Project name
        total : int, optional
            Number of issues to go thorugh, None goes through all, by default None
        maxResults : int, optional
            Number of maximal results on a page, by default 1000
        startAt : int, optional
            Index to start paging at, by default 0

        Yields
        ------
        Tuple[Issue,Dict]
            An issue from the project
        """
        if total == None:
            params = {
                "jql": f"project={project}",
                "maxResults": 0,
                "startAt": 0,
            }
            r = requests.get(SEARCHURL, params=params, auth=self.auth)

        # Log error if response status is not 200 OK
        if r.status_code != 200:
            self.logger.error("Project %s not found", project)
            return
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
            "fields": "description,comment,status,issuetype,created,updated,resolved,worklog",
        }

        issues = []
        # Go until total has been reached
        while startAt < total:
            r = requests.get(SEARCHURL, params=params, auth=self.auth)
            self.logger.info(
                "Downloading issues from %s project, startAt: %d, %.2f%% downloaded.",
                project,
                startAt,
                float(100 * startAt / total),
            )
            jsondict = r.json()
            # convert issue into Issue dataclass object
            for issue in jsondict["issues"]:
                issue_object = to_dataclass(Issue, issue)

                # self.gather_links_for_issue(issue_object, issue)
                # issues.append(issue_object)
                yield issue_object, issue
            startAt += maxResults
            params["startAt"] = startAt
        self.logger.info("Downloaded 100%% of issues from %s project.", project)

    @log_time(loggerName="crawler")
    def gather_issues(self, project: str, total=None, maxResults=1000, startAt=0):
        """Gather issues for a project and gather the links to github pull requests and commits from the issue

        Parameters
        ----------
        project : str
            Project name
        total : int, optional
            Number of issues to go thorugh, None goes through all, by default None
        maxResults : int, optional
            Number of maximal results on a page, by default 1000
        startAt : int, optional
            Index to start paging at, by default 0

        Returns
        -------
        List[Issue]
            List of issues
        """
        issues = []

        for issue, issue_dict in self.iterate_through_issues(
            project, total, maxResults, startAt
        ):
            self.gather_links_for_issue(issue, issue_dict)
            issues.append(issue)

        return issues

    def get_projects(self):
        """Get all information for all projects from JIRA

        Returns
        -------
        Projects
            Projects object with all the available projects
        """
        r = requests.get(BASEURL + "project", auth=self.auth)
        projects = [to_dataclass(Project, project) for project in r.json()]
        return Projects(projects)

    def gather_all_projects(self, projectsDir: str):
        """Gather all projects from JIRA with issues and save into a JSON into the directory

        Parameters
        ----------
        projectsDir : str
            Path to a directory where the project JSON-s are saved
        """
        projects = self.get_projects()
        for project in projects.projects:
            # TST project causes some problems
            if (
                not os.path.exists(os.path.join(projectsDir, project.key) + ".json")
                and project.key != "TST"
            ):
                project.issues = self.gather_issues(
                    project.key,
                    None,
                    1000,
                    0,
                )
                with open(
                    os.path.join(projectsDir, project.key) + ".json", "wt"
                ) as file:
                    file.write(project.to_json(indent=4))

    def find_issue(self, project: Project, target: Issue):
        """Finds an issue with the same self link in the project"""
        if project is None:
            return None
        for issue in project.issues:
            if issue.self == target.self:
                return issue

    @log_time(loggerName="crawler")
    def gather_commits_for_project(
        self,
        project: Project,
        savePath: str,
        savedProjectPath: str,
        saveFrequency: int = 1000,
    ):
        """Gathers all the Github commits for a project based on the
            pull request and commit links in the Issue objects

        Parameters
        ----------
        project : Project
            _description_
        savePath : str
            _description_
        savedProjectPath : str
            _description_
        saveFrequency : int, optional
            _description_, by default 1000
        """
        self.logger.info("Gathering commits for %s project", project.key)
        existingProject = None
        newProject = Project(
            project.self,
            project.id,
            project.key,
            project.name,
            project.url,
            project.repo_url,
            [],
        )
        # Check if there is a saved version
        try:
            if os.path.exists(savedProjectPath):
                with open(savedProjectPath, "rt") as file:
                    existingProject = Project.from_json(file.read())
                newProject = Project(
                    existingProject.self,
                    existingProject.id,
                    existingProject.key,
                    existingProject.name,
                    existingProject.url,
                    existingProject.repo_url,
                    [],
                )
        except Exception as e:
            pass
        futures = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            for index, issue in enumerate(project.issues):
                existingIssue = self.find_issue(existingProject, issue)
                # Check if the saved version is complete
                if (
                    existingIssue is None
                    or existingIssue.commits is None
                    or (
                        len(existingIssue.commits) == 0
                        and (
                            len(existingIssue.prlinks) > 0
                            or len(existingIssue.commitlinks) > 0
                        )
                    )
                ):
                    futures.append(
                        executor.submit(self.gather_commits_for_issue, issue)
                    )
                    newProject.issues.append(issue)
                else:
                    newProject.issues.append(existingIssue)
                # Save the current state
                if index % saveFrequency == 0:
                    with open(savePath.split(".")[0] + f"_{index}.json", "wt") as file:
                        file.write(newProject.to_json(indent=4))
        wait(futures)
        project.issues = newProject.issues
        self.logger.info("Request counter state: %s", str(self.github.requestCounter))

    @log_time(loggerName="crawler")
    def gather_commits_for_issue(self, issue: Issue):
        """Gathers all the commits for an issue

        Parameters
        ----------
        issue : Issue
            Issue to gather the commits for
        """
        if (len(issue.prlinks)) > 0:
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

    def check_all_issues(self, project: Project):
        """Check if a project has all the issues, which are currently in the Apache JIRA

        Parameters
        ----------
        project : Project
            Project to be checked

        Returns
        -------
        _type_
            List of issues, which are not in the project
        """
        notdownloaded = []
        self.logger.info("Checking project %s", project.key)
        issuenames = [issue.self for issue in project.issues]
        for issue, _ in self.iterate_through_issues(project.key):
            if issue.self not in issuenames:
                notdownloaded.append(issue.self)

        return notdownloaded

    def expand_with_not_downloaded(self, project: Project):
        """Expand the project to have all the issues, which are in the Apache JIRA

        Parameters
        ----------
        project : Project
            Project to be expanded
        """
        issuenames = [issue.self for issue in project.issues]
        for issue, issue_dict in self.iterate_through_issues(project.key):
            if issue.self not in issuenames:
                issue = to_dataclass(Issue, issue_dict)
                self.gather_links_for_issue(issue, issue_dict)
                project.issues.append(issue)

    def expand_projects(self, projectsDir: str, saveDir: str):
        """Expand all projects located in a directory and save the expanded into another directory

        Parameters
        ----------
        projectsDir : str
            Path to a directory, where the projects are in json format
        saveDir : str
            Path to a directory, where the expanded projects will be saved
        """
        if not os.path.exists(saveDir):
            os.mkdir(saveDir)
        for filename in os.listdir(projectsDir):
            print(filename)
            fullpath = os.path.join(projectsDir, filename)
            with open(fullpath, "rt") as file:
                project = Project.from_json(file.read())

            self.expand_with_not_downloaded(project)

            with open(os.path.join(saveDir, filename), "wt") as file:
                file.write(project.to_json(indent=4))


def download_files_for_repositories(
    repositoriesDir: str, rootDirectory: str, saveDir: str, onlyRelevant=True
):
    """Downloads all files for all the commits in a repository concurrently

    Parameters
    ----------
    repositoriesDir : str
        Path to a directory where the jsons with repository info are located
    rootDirectory : str
        A root directory where all the files are saved into a directory with the repository's name
    saveDir : str
        Path to a directory where the updated repositories are saved with the path to the commits
    onlyRelevant : bool, optional
        To download only those pull requests, which have an associated issue, by default True
    """
    crawler = Crawler(
        "crawler_multithreaded",
        "log/crawler_multithreaded.log",
        auth=("meszarosp", "^N97#ZnTaLhpf7Cv3EtWVQCdPhT9kv"),
    )
    crawler.download_files_for_repositories(
        repositoriesDir, rootDirectory, saveDir, onlyRelevant
    )


def main():
    jsonargparse.CLI(download_files_for_repositories)
    # init_logger("githubcrawler", "crawler_multithread.log")
    # crawler = Crawler(
    #     "crawler_multithreaded",
    #     "log/crawler_multithreaded.log",
    #     auth=("meszarosp", "^N97#ZnTaLhpf7Cv3EtWVQCdPhT9kv"),
    # )

    # crawler.download_files_for_repositories(
    #     "repositories_with_issues_20221116",
    #     "commits_20221116",
    #     "repositories_with_issues_20221116_with_commits",
    #     True,
    # )

    # crawler.expand_projects(
    #     "projects_all_infos",
    #     "projects_all_infos_expanded_20221116",
    # )


if __name__ == "__main__":
    main()
