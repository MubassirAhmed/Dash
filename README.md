# Python dashboard for analyzing Linkedin jobs

## A fully functional dashboard, mining scraped data from linked, and built with Python and Dash

This product is built as part of an ELT pipeline, to demonstrate an end-user usecase for the fact and dimension tables generated in the snowflake warehouse. It filters, organizes, and presents jobs in a table based on the years of experience required. This in particular is an issue as many jobs are labled as entry-level jobs, yet ask for several years of experience (YoE); at the same time, many jobs do not explicitly mention any strict YoE requirements but are currently not searchable with the linkedin search engine.

Furthermore, the app creates visualizations from these tables to illustrate other useful insights such as:

* Technical skills that are currently most in demand, i.e., have most number of jobs
* The demand for years of experience
* Time of day, and day of the week when most number of jobs are posted
