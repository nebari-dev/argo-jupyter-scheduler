# argo-workflows-executor

[![PyPI - Version](https://img.shields.io/pypi/v/argo-workflows-executor.svg)](https://pypi.org/project/argo-workflows-executor)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/argo-workflows-executor.svg)](https://pypi.org/project/argo-workflows-executor)

-----

**Table of Contents**

- [argo-workflows-executor](#argo-workflows-executor)
  - [Installation](#installation)
  - [What is it?](#what-is-it)
  - [A deeper dive](#a-deeper-dive)
    - [`Job`](#job)
    - [`Job Definition`](#job-definition)
    - [Internals](#internals)
  - [License](#license)

**Argo-Jupyter-Scheduler**

Submit longing running notebooks to run without the need to keep your JupyterLab server running. And submit a notebook to run on a specified schedule.

## Installation

```console
pip install argo-workflows-executor
```

## What is it?

Argo-Jupyter-Scheduler is a plugin to the Jupyter-Scheduler JupyterLab extension. 

What does that mean?

This means this is an application that gets installed in the JupyterLab base image and runs as an extension in JupyterLab. Specifically, you will see this icon from the JupyterHub Launcher window: 

<add screenshot>

And this icon on the toolbar of your Jupyter Notebook:

<add screenshot>

This also means, as a lab extension, this application is running within each user's separate JupyterLab server. The record of the notebooks you've submitted is specific to you and you only. There is no central Jupyter-Scheduler. 

However, instead of using the base Jupyter-Scheduler, we are using **Argo-Jupyter-Scheduler**. 

Why?

If you want to run your Jupyter Notebook on a schedule, you need to be assured that the Notebook will be executed at the times you specified. The fundamental limitation with Jupyter-Scheduler is that when your JupyterLab server is not running, Jupyter-Scheduler is not running. Well, then the Notebooks you had scheduled won't run. (As an aside, the trouble with Notebooks that you want to run right now, is that if the JupyterLab server is down, then how will the status of the notebook run be recorded?)

The solution is Argo-Jupyter-Scheduler: Jupyter-Scheduler front-end with an Argo-Workflows back-end.

## A deeper dive

In the Jupyter-Scheduler lab extension, you can create two things, a `Job` and a `Job Definition`.

### `Job`

A `Job`, or notebook job, is when you submit your notebook to run.

In Argo-Jupyter-Scheduler, this `Job` translates into a `Workflow` in Argo-Workflows. So when you create a `Job`, your notebook job will create a Workflow that will run regardless of whether your JupyterLab server is.

> At the moment, permission to submit Jobs is required which are controlled by Keycloak roles for the `argo-server-sso` client. If your user has either the `argo-admin` or the `argo-developer` roles, they will be permitted to create and submit Jobs (and Job Definitions).

We are relying on the Nebari Workflow Controller to ensure the user's home directory and conda-store environments are mounted to the Workflow. This allows us to ensure:
- the local data used by the notebook job is accessible
- the output of the notebook can be saved locally
- when the conda environment used gets updated, it is also updated for the notebook job (helpful for scheduled jobs)
- the node instance type and image you submit your notebook job from are the same ones used by the workflow


### `Job Definition`

A `Job-Definition` is simply a way to create to Jobs that run on a specified schedule.

In Argo-Jupyter-Scheduler, this `Job Definition` translate into a `Cron-Workflow` in Argo-Worflows. So when you create a `Job Definition`, you create a `Cron-Workflow` that creates a `Workflow` to run according to the specified schedule.

A `Job` is to `Workflow` as `Job Definition` is to `Cron-Workflow`.



### Internals

Jupyter-Scheduler creates and uses a `scheduler.sqlite` database to manage and keep track of the Jobs and Job Definitions. If you can ensure this database is accessible and can be updated when the status of a job or a job definition change, then you can ensure the view the user sees from JupyterLab match is accurate.

> By default this database is located at `~/.local/share/jupyter/scheduler.sqlite` but this is a trailet that can be modified. And since we know where this database is, and it's accessible, we can update the database directly from the workflow it's self.

To acommplish this, the workflow runs in two steps. First the workflow runs the notebook, using `papermill` and the conda environment specified. And second, depending on the status of this notebook run, updates the database with this status.

And when a job definition is created, a corresponding cron-workflow is created. To ensure the database is properly updated, the workflow that the cron-workflow creates has three steps. First, create a job record in the database with a status of `IN PROGRESS`. Second, run the notebook, again using `papermill` and the conda environment specified. And third, update the newly created job record with the status of the notebook run.

## License

`argo-workflows-executor` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
