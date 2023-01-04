# [Linkedin Jobs Analysis Dashboard](https://easy-bottles-grin-34-125-254-54.loca.lt)

## An [interactive dashboard](https://easy-bottles-grin-34-125-254-54.loca.lt) built with Python and Dash that mines scraped data from Linkedin 

This app is built as part of an [ELT pipeline](https://github.com/MubassirAhmed/ELT-Data-Pipeline) to showcase an example end-user data product. 

It filters and tags jobs based on the years of experience (YoE) mentioned in the job descriptions. This gives a more concrete way of searching for jobs as Linkedin uses arbitrary tags such as 'entry-level', 'associate', 'mid-senior level', etc. to organize their job postings.

This in particular, solves a real-world issue, as many postings tagged as entry-level jobs on Linkedin ask for several YoE, and it's not possible to filter them using Linkedin's search. During the search process, sorting through entry-level roles manually becomes extremely time consuming, especially when hundreds of new jobs are posted everyday, making it impractical to go through all of them. 

The app also creates other useful visualizations such as:

* Technical skills that are currently most in demand
* The demand for years of experience
* Time of day, and day of the week when most number of jobs are posted

## How to use:
1.The first table shows new jobs posted in the last 5 hours. The jobs can be sorted by their current number of applications received.

![Alt Text](https://media.giphy.com/media/ZHNF7pWf8732V9dpoM/giphy.gif)

2. Jobs in the first table can be filtered by YoE by clicking on the corresponding bar on the bar chart.

![Alt Text](https://media.giphy.com/media/T5BTftQVy2sMqcaFF6/giphy.gif)

3. You can filter results in the 2nd table using any keyword. This enables you to filter job titles, and search for keywords in job descriptions.

![Alt Text](https://media.giphy.com/media/RWo6c6dOWt7W2HJiYd/giphy.gif)

4. The bar charts display aggregates, so the time range for aggregating can be changed with the slider and radio buttons.

![Alt Text](https://media.giphy.com/media/ZjhLBSry5UfLPGoKfE/giphy.gif)


### To install and run locally on Unix systems:
1. Install Python 3
2. run the following command in your terminal to install the app:

	```git clone https://github.com/MubassirAhmed/Dash.git && cd Dash && pip3 install -r requirements.txt ```

3.Run the following to start the app:

	```python3 DashJobsAnalysis.py```


### Known issues
* Cards are not spaced out properly in smaller screens. Need to add padding in CSS
* Row height in second table needs to be fixed, currently they take up the full space necessary for displaying entire job descriptions
* Filter input box is difficult to see, needs the CSS color changed

