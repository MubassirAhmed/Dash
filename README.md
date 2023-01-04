# Dashboard for analyzing Linkedin jobs

## An interactive dashboard, mining scraped data from linked, built with Python and Dash

This app is built as part of an [ELT pipeline](https://github.com/MubassirAhmed/ELT-Data-Pipeline), showcasing an example end-user product built from the fact and dimension tables created previously in the snowflake warehouse. 

It filters and tags jobs based on the years of experience (YoE) mentioned in the job descriptions. This gives a more concrete way of searching for jobs as Linkedin uses arbitrary tags such as 'entry-level', 'associate', 'mid-senior level', etc. to organize their job postings.

This in particular, solves a real-world issue, as many postings tagged as entry-level jobs on Linkedin ask for several YoE, and it's not possible to filter them using Linkedin's search. During the search process, sorting through entry-level roles manually becomes extremely time consuming, especially when hundreds of new jobs are posted everyday, making it impractical to go through all of them. 

On the flip side, there are also many jobs that do not explicitly mention any specific YoE requirements, but again, it's search these using Linkedin's standard search. This app tags such jobs with a '0' YoE requirment and helps to search for such jobs.

The app also creates other visualizations from the datawarehouse views and tables to illustrate useful insights such as:

* Technical skills that are currently most in demand
* The demand for years of experience
* Time of day, and day of the week when most number of jobs are posted

## How to use:
1.The first table shows new jobs posted in the last 5 hours. The jobs can be sorted by the number of applications received.
![Alt Text](https://media.giphy.com/media/ZHNF7pWf8732V9dpoM/giphy.gif)

2. Jobs in the first table can be filtered by YoE by clicking on the corresponding bar on the bar chart.
![Alt Text](https://media.giphy.com/media/T5BTftQVy2sMqcaFF6/giphy.gif)

3. The bar charts display aggregates, so the time range for aggregating can be changed with the slider and radio buttons.
![Alt Text](https://media.giphy.com/media/ZHNF7pWf8732V9dpoM/giphy.gif)

4. You can filter results in the 2nd table using any keyword. This enables you to filter job titles, and search for keywords in job descriptions.
![Alt Text](https://media.giphy.com/media/ZHNF7pWf8732V9dpoM/giphy.gif)


### The app can be run locally like this:
The app is currently deployed at dashboard.renderme with CI/CD, and will later be migrated to a GCP VM instance once the networking settings and CI/CD tools are figured out


### Known issues
* App is slow due to free tier server hosting (with 512 MB RAM). Will migrate to VM instance with more RAM on GCP
* Filter input box is difficult to see, needs the CSS color changed
* Cards are not spaced out properly in smaller screens. Need to add padding in CSS
* Row height in second table needs to be fixed, currently they take up the full space necessary for displaying entire job descriptions

