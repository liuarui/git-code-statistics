#!/usr/bin/env python
# coding=utf-8

import gitlab
import pandas as pd
import os
import json

# GitLab configuration
url = 'your_gitlab_url'
private_token = 'your_private_token'
gl = gitlab.Gitlab(url, private_token=private_token)
gl.auth()

# Time range for commits
start_time = '2024-01-01T00:00:00Z'
end_time = '2024-12-31T23:59:59Z'
processed_projects_file = 'processed_projects.json'


def load_processed_projects():
    """Load the set of processed projects from a JSON file."""
    if os.path.exists(processed_projects_file):
        with open(processed_projects_file, 'r') as f:
            return set(json.load(f))
    return set()


def save_processed_projects(processed_projects):
    """Save the set of processed projects to a JSON file."""
    with open(processed_projects_file, 'w') as f:
        json.dump(list(processed_projects), f)


def get_gitlab(processed_projects):
    """Fetch project data from GitLab and process commits."""
    projects = gl.projects.list(owned=True, all=True)
    for num, project in enumerate(projects, start=1):
        if project.path_with_namespace in processed_projects:
            continue

        print(f"查看了{num}个项目")
        process_project_commits(project, processed_projects)


def process_project_commits(project, processed_projects):
    """Process commits for a given project."""
    commit_data = []
    try:
        branch = project.branches.get('master') # stat branch
        commits = project.commits.list(all=True, query_parameters={
            'since': start_time, 'until': end_time, 'ref_name': branch.name
        })

        for commit in commits:
            try:
                commit_info = project.commits.get(commit.id)
                commit_data.append({
                    "projectName": project.path_with_namespace,
                    "authorName": commit_info.author_name,
                    "branch": branch.name,
                    "additions": commit_info.stats["additions"],
                    "deletions": commit_info.stats["deletions"],
                    "commitNum": commit_info.stats["total"]
                })
            except Exception as e:
                print(f"有错误, 请检查: {e}")

    except gitlab.exceptions.GitlabGetError:
        print(f"项目 {project.path_with_namespace} 没有 master 分支")

    processed_projects.add(project.path_with_namespace)
    save_processed_projects(processed_projects)
    write_to_csv(commit_data)


def aggregate_data(commit_data):
    """Aggregate commit data by project, author, and branch."""
    aggregated = {}
    for entry in commit_data:
        key = f"{entry['projectName']}{entry['authorName']}{entry['branch']}"
        if key not in aggregated:
            aggregated[key] = entry
            aggregated[key]["commitTotal"] = 1
        else:
            aggregated[key]["additions"] += entry["additions"]
            aggregated[key]["deletions"] += entry["deletions"]
            aggregated[key]["commitNum"] += entry["commitNum"]
            aggregated[key]["commitTotal"] += 1

    return [
        {
            "项目名": v["projectName"],
            "开发者": v["authorName"],
            "分支": v["branch"],
            "添加代码行数": v["additions"],
            "删除代码行数": v["deletions"],
            "提交总行数": v["commitNum"],
            "提交次数": v["commitTotal"]
        }
        for v in aggregated.values()
    ]


def write_to_csv(commit_data):
    """Write aggregated commit data to a CSV file."""
    df = pd.DataFrame(aggregate_data(commit_data), columns=[
        "项目名", "开发者", "分支", "添加代码行数", "删除代码行数", "提交总行数", "提交次数"
    ])
    file_mode = 'a' if os.path.exists("./gitlab.csv") else 'w'
    df.to_csv("./gitlab.csv", mode=file_mode, header=(file_mode == 'w'), index=False, encoding="utf_8_sig")


if __name__ == "__main__":
    processed_projects = load_processed_projects()
    get_gitlab(processed_projects)
