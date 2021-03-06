# Sparkify Cassandra ETL

## Table of contents

- [Overview](#overview)
- [Dataset](#dataset)
- [Project Setup](#project-setup)

## Overview

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a _*Cassandra*_ database with tables designed to optimize queries on song play analysis. Thus, the goal is to **create a database schema and ETL pipeline** for this analysis.

## Dataset

#### Event Data Dataset

The second dataset consists of log files in CSV format generated by an event simulator based on the songs in the dataset in the previous project. These simulate activity logs from a music streaming app based on specified configurations.

## Project Structure

```
├── event_data
├── notebook.ipynb: notebook containing the project code.
```

## Project Setup

1. Make sure to have [Poetry Package Manager](https://python-poetry.org/) installed.

2. Run `poetry install` to install the dependencies.

3. Run `poetry run jupyter notebook notebook.ipynb` to run the notebook.
