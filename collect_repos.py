from concurrent.futures import ThreadPoolExecutor, wait
import os
from jira_github_dataclasses import *
from crawler_multithreaded import PULLREQUESTORCOMMITREGEX
import itertools
import re
import json
from utils import init_logger
import difflib

logger = init_logger("connecter", logfile="log/connecter.log")

# Ignore these issue keys in the pull request titles
IGNOREKEY = (
    "UTF",
    "CVE",
    "JDK",
    "RFC",
    "AIP",
    "BACKPORT",
    "BP",
    "BZ",
    "PEP",
    "SHA",
    "AR",
    "GMT",
)
# Rename these mistyped or deprecated issue keys to the correct ones
# At 0.9 similarity threshold
RENAME = {
    "APEX": "APEXCORE",
    "AMABRI": "AMBARI",
    "GUACAOMLE": "GUACAMOLE",
    "GUACMAOLE": "GUACAMOLE",
    "GUACOMOLE": "GUACAMOLE",
    "TRAFDOION": "TRAFODION",
    "TRAFIDION": "TRAFODION",
    "TRAFOFION": "TRAFODION",
    "YUNIKONR": "YUNIKORN",
    "YINIKORN": "YUNIKORN",
    "THIRFT": "THRIFT",
    "TINEKRPOP": "TINKERPOP",
    "TEHPRA": "TEPHRA",
    "CACLITE": "CALCITE",
    "FINEARCT": "FINERACT",
    "FINERCAT": "FINERACT",
    "FILNK": "FLINK",
    "IGNTIE": "IGNITE",
    "FACLON": "FALCON",
    "INGITE": "IGNITE",
    "IGNITW": "IGNITE",
    "ARAI": "ARIA",
    "ATLS": "ATLAS",
    "FINK": "FLINK",
    "FLIN": "FLINK",
    "FLNK": "FLINK",
    "HUID": "HUDI",
    "MIME": "MIME4J",
    "MLHR": "APEXMALHAR",
    "PLSR": "PULSAR",
    "HBAE": "HABSE",
    "STROM": "STORM",
    "COUCH": "COUCHDB",
    "COBBLIN": "GOBBLIN",
    "SUBMAEINE": "SUBMARINE",
    "AIFRLOW": "AIRFLOW",
    "AIRLFOW": "AIRFLOW",
    "GUAC": "GUACAMOLE",
    "GOEDO": "GEODE",
    "KAFA": "KAFKA",
    "FLI": "FLINK",
    "KYILN": "KYLIN",
    "IOTFB": "IOTDB",
    "SIGNA": "SINGA",
    "VY": "LIVY",
    "NIIFI": "NIFI",
    "NUTH": "NUTCH",
    "KAFAK": "KAFKA",
    "GOEDE": "GEODE",
    "SYSTEML": "SYSTEMML",
    "NLP": "OPENNLP",
    "SUBAMRINE": "SUBMARINE",
    "KAKFA": "KAFKA",
    "JAME": "JAMES",
    "GEOE": "GEODE",
    "PARUQET": "PARQUET",
    "GEDOE": "GEODE",
    "INLONF": "INLONG",
    "INK": "FLINK",
    "FODION:": "TRAFODION",
    "KFKA": "KAFKA",
    "PARQURT": "PARQUET",
}


def collect_repos(projectsDir, projectPath, repoPath, permittedOwners=None):
    projects = []
    allrepos = set()
    for filename in os.listdir(projectsDir):
        fullpath = os.path.join(projectsDir, filename)
        with open(fullpath, "rt") as file:
            project = Project.from_json(file.read())

        repos = set()
        for issue in project.issues:
            for link in itertools.chain(issue.prlinks, issue.commitlinks):
                try:
                    reponame = re.match(PULLREQUESTORCOMMITREGEX, link).groupdict()[
                        "reponame"
                    ]
                    owner, name = reponame.split("/")[0:2]
                    if permittedOwners is None or owner in permittedOwners:
                        repos.add("/".join((owner, name)))
                except AttributeError as ae:
                    print(link)
                    match = re.match(PULLREQUESTORCOMMITREGEX, link)
                    if match is not None:
                        print(match.groupdict())
                except ValueError as ve:
                    continue
        allrepos = allrepos.union(repos)
        project.reponames = list(repos)
        project.issues = []
        projects.append(project)
    projects = Projects(projects)
    with open(projectPath, "wt") as file:
        file.write(projects.to_json(indent=4))

    with open(repoPath, "wt") as file:
        json.dump(list(allrepos), file, indent=4)


def connect_repo_with_project(
    projects: List[Project],
    repository: Repository,
    savePath: str,
    projectsDir: str,
    projectNames: List[str],
):
    """Connect a repository's pull requests with its corresponding JIRA issue
        based on the issue key contained in the pull request's title

    Parameters
    ----------
    projects : List[Project]
        List of project objects
    repository : Repository
        A repository to connect for which to find the corresponding issues
    savePath : str
        Path to a directory, where the connected projects are saved
    projectsDir : str
        Path to a directory, where the projects are saved in json format
    projectNames : List[str]
        List of all the projects' names
    """

    for pull in repository.pulls:
        if pull.issue_key is None:
            continue
        pull.issue = []
        for issue_key in pull.issue_key:
            try:
                # Split the key, PROJECT-1234 format
                projectName = issue_key.split("-")[0]
                originalProjectName = projectName
                number = issue_key.split("-")[1]
                if projectName in IGNOREKEY:
                    continue
                # rename the name or find the closest project
                projectName = RENAME.get(projectName, projectName)
                closestProjectName = difflib.get_close_matches(
                    projectName, projectNames, 1, 0.9
                )
                # if there are no projects similar enough, then don't change
                closestProjectName = (
                    closestProjectName[0]
                    if len(closestProjectName) > 0
                    else projectName
                )
                if closestProjectName not in projects:
                    # Read the project
                    with open(
                        os.path.join(projectsDir, closestProjectName + ".json")
                    ) as file:
                        project = Project.from_json(file.read())
                        projects[closestProjectName] = project
                project = projects[closestProjectName]
                issue_key = closestProjectName + "-" + str(number)
                # Add issue to the pull requests
                issue = [issue for issue in project.issues if issue.key == issue_key]
                pull.issue.extend(issue)
                if closestProjectName != originalProjectName:
                    logger.info(
                        "%s key replaced with %s",
                        originalProjectName,
                        closestProjectName,
                    )
            except FileNotFoundError:
                logger.error(
                    "Repo: %s, pull: %d, key: %s. JIRA project not found.",
                    repository.full_name,
                    pull.number,
                    pull.issue_key,
                )
            except IndexError:
                logger.error(
                    "Repo: %s, pull: %d, key: %s. JIRA issue not found in the project.",
                    repository.full_name,
                    pull.number,
                    pull.issue_key,
                )
            except Exception:
                logger.error(
                    "Repo: %s, pull: %d, key: %s. Error:",
                    repository.full_name,
                    pull.number,
                    pull.issue_key,
                    exc_info=True,
                )
    with open(savePath, "wt") as file:
        file.write(repository.to_json(indent=4))


def connect_repos_with_projects(repositoriesDir: str, projectsDir: str, saveDir: str):
    """Connect all repositories' pull requests with its corresponding JIRA issue
        contained in a directory
        based on the issue key contained in the pull request's title

    Parameters
    ----------
    repositoriesDir : str
        Path to a directory, where the repositories are ,located in json format
    projectsDir : str
        Path to a directory, where the projects are saved in json format
    saveDir : str
        Path to a directory, where the connected projects are saved
    """
    projects = dict()
    futures = []
    projectnames = [filename.split(".")[0] for filename in os.listdir(projectsDir)]

    with ThreadPoolExecutor(max_workers=20) as executor:
        for repositoryfilename in os.listdir(repositoriesDir):
            if os.path.exists(os.path.join(saveDir, repositoryfilename)):
                continue
            with open(os.path.join(repositoriesDir, repositoryfilename), "rt") as file:
                repository = Repository.from_json(file.read())
            print(repositoryfilename)
            futures.append(
                executor.submit(
                    connect_repo_with_project,
                    projects,
                    repository,
                    os.path.join(saveDir, repositoryfilename),
                    projectsDir,
                    projectnames,
                )
            )
    wait(futures)


# collect_repos(
#     "projects_all_infos",
#     "all_projects_with_github_repos.json",
#     "all_repos_from_jira.json",
#     None,
# )

connect_repos_with_projects(
    "repositories_20221116",
    "projects_all_infos_expanded_20221116",
    "repositories_with_issues_20221116",
)

# with open(
#     os.path.join("repositories_with_issues_20221126", "airavata.json"), "rt"
# ) as file:
#     repository = Repository.from_json(file.read())
# projects = dict()
# projectnames = [
#     filename.split(".")[0]
#     for filename in os.listdir("projects_all_infos_expanded_20221126")
# ]
# connect_repo_with_project(
#     projects,
#     repository,
#     "repositories_with_issues_test",
#     "projects_all_infos_expanded_20221126",
#     projectnames,
# )
