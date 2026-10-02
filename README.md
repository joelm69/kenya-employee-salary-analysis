# Kenya Employee Salary Analysis

## An End-to-End Data Engineering & Analytics Pipeline

## Project Overview

This project analyses employee salary and workforce data from a Kenyan organisation to understand salary distribution, departmental pay differences, employee characteristics and performance patterns.

The project was built as an end-to-end data engineering and analytics pipeline, taking raw CSV data through data validation, cleaning, event processing with Apache Kafka, cloud data warehousing with Google BigQuery and an interactive Streamlit application.

## Business Problem

Organisations need reliable workforce data to understand compensation patterns, employee characteristics and potential salary inconsistencies.

The objective of this project was to transform raw employee and department data into reliable, analysis-ready information that can support workforce and compensation analysis.

## Objectives

- Validate and clean employee and department data
- Build an end-to-end data engineering pipeline
- Process employee records through Apache Kafka
- Use Docker to containerise the Kafka environment
- Store raw and analytical data in Google BigQuery
- Transform and analyse data using SQL
- Analyse salary patterns across departments
- Examine relationships between salary, age, and performance
- Identify potential salary outliers
- Build an interactive Streamlit application for analysis and exploration

## Data

The project uses two CSV datasets.

### Employee Data

The employee dataset contains **420 records** and includes information such as:

- Employee ID
- Full name
- Gender
- Department
- Job title
- County
- Hire date
- Salary
- Age
- Performance rating

### Department Data

A department reference dataset is used to validate department relationships and support the analytical transformation.

## Data Engineering Pipeline

CSV Files
    ↓
Python
    ↓
Apache Kafka
    ↓
Google BigQuery
    ↓
Streamlit

##Business Recommendations
Based on the analysis, the following areas could be investigated:
Review departmental compensation structures to understand the factors contributing to differences in salary levels across departments.
- Investigate salary outliers to determine whether unusually high or low salaries are explained by job title, experience, responsibilities or other  factors.
- Avoid relying on performance ratings alone for compensation decisions, as the analysis indicates only a weak linear relationship between performance rating and salary.
- Conduct regular salary reviews to identify potential inconsistencies in compensation within and across departments.
- Incorporate additional variables into future analysis, particularly experience, job level, tenure, and responsibilities, to provide a more complete understanding of salary differences.
- Use the analytical dashboard for ongoing monitoring so management can track changes in salary distributions and workforce characteristics as new data becomes available.
