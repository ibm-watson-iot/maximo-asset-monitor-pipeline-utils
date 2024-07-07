# monitor-pipeline-utils

Monitor-pipeline-utils is a python module to deal with two use cases:

1. Create pipeline elements for space planning of a building
2. Render an asset's pipeline as directed acyclic graph (DAG)


## How to set up a space planning dashboard in Monitor

1. Objective
2. Flowchart of pipeline
3. Detailed steps involved in building the pipeline

### Objective

The objective show free capacity in a building based on historic occupancy data. We recommend to show the number of seats that are almost always unused along with the overall capacity of the building.
To underpin the number of unused seats we recommend to set up a histogram-like bar chart that shows how often maximum occupancy was reached. <br><br>

![image](https://media.github.ibm.com/user/370291/files/5ad2a6c1-557a-437b-bc35-7a6859c105f4)

### Steps for the pipeline setup: 

·	Select Building, floors and spaces from the hierarchy list.

·	Implement Rolling Average Using Custom window sizes (15 & 30)

·	Aggregated with expression taking Max of hourly window

·	Aggregated with expression by taking granularity as “daily”  

·	Aggregate the daily max at floor and building level

<br>
The KPI functions for these computations are deployed with the python code in this repository.

Example:

`python main.py --tenant-id main --site IBMCLOUD_TEST3 --deploy Green`

shows a selection list containing all locations containing "Green" as substring in tenant "main" and site "IBMCLOUD_TEST3". Selecting a building location will deploy all KPI functions required for space planning.

Since we provide 10 bins for this bar chart and encode them as time series data for the last 10 days, we need to configure the “space utilization” dashboard to render the last 10 days

#### Prerequisites

In order to use the script you need the credentials of the Monitor instance as json document.

Example

```
{
    "baseurl": "https://main.api.monitor.monitortest.ibmmasmonitor.com",
    "apikey": "<APIKEY>",
    "apitoken": "<APITOKEN>"
}
```


### Configuring the dashboard

Navigate to the building in the location hierarchy in Maximo Asset Monitor's Setup menu after deploying the KPI functions for space planning (as described in the previous section).

As in the example below click the selected building,

<image src="https://media.github.ibm.com/user/370291/files/4eecc0f8-07ce-4a5e-9f03-3bdc6dc933e1" height="70%" width="70%"/>

select the "Dashboard" entry in the menu

<image src="https://media.github.ibm.com/user/370291/files/1d98866b-b647-416a-8bf7-8946a3c66492" height="70%" width="70%"/>

On the dashboards page, select "Add Dashboard" to create a new dashboard for space planning.

<image src="https://media.github.ibm.com/user/370291/files/279b450b-b47f-4a61-b4c4-ee989982ba91" height="70%" width="70%"/>

The next page will show an empty dashboard with widgets shown on the left side bar for editing/creating a new graph

<image src="https://media.github.ibm.com/user/370291/files/69c89c03-004a-4168-9854-ae90f09815ad" height="70%" width="70%"/>

Add a bar chart and use the built-in JSON editorto edit the card configuration as shown in the screenshot below

<image src="https://media.github.ibm.com/user/370291/files/20ad65dc-bfe0-40d3-874d-8c02af506316" height="70%" width="70%"/>
 
This is how we recommend to set up your space utilization dashboard

<image src="https://media.github.ibm.com/user/370291/files/d492c180-ce6e-4dec-a9a1-b4718878c095" height="70%" width="70%"/>

The space utilization graph is shown below:

The unused seats tile is configured as KPI value

<image src="https://media.github.ibm.com/user/370291/files/3f0c5e76-d7c6-4622-9795-b23841b6cf01" height="70%" width="70%"/>

similar to capacity
 
<image src="https://media.github.ibm.com/user/370291/files/3fc7e44e-ea96-4665-95d5-28f11b55baae" height="70%" width="70%"/>

The histogram like bar chart, a simple bar chart widget, requires editing its configuration in the JSON editor as shown before.


### DAG rendering

`python main.py --tenant-id main --site IBMCLOUD_TEST3 --render Green`

Select a location in site IBMCLOUD_TEST3 from all locations with "Green" as substring and their sublocations and render its pipeline as DAG

`python main.py --tenant-id main --site IBMCLOUD_TEST3 --render 101 --td`

Same as above but select locations with "101" as substring and render the DAG top-down instead of left-right

#### Prerequisites

Again, you need the credentials of the Monitor instance as json document to run this command.

Example

```
{
    "baseurl": "https://main.api.monitor.monitortest.ibmmasmonitor.com",
    "apikey": "<APIKEY>",
    "apitoken": "<APITOKEN>"
}
```

### How does it look like

![image](https://github.com/ibm-watson-iot/maximo-asset-monitor-pipeline-utils/assets/11428923/a63ed977-dbb6-4690-bf8e-0d1b23adcb5e)


### Appendix

#### Description of the KPI functions

Steps to set up the space planning component in UI

Navigating to the building in the MAS Monitor hierarchy

Step 1: 

-	Selection of the building, which is done by first selecting the setup option from the main monitor page.

Step 2:

-	Select the floor for which the metrics are to be created.

Step 3:
-	Creation of metrics on the space level
While we also deliver a script to set up the pipeline functions for the space level, we describe the ‘manual’ steps in detail.

  Smoothening of the trend: A rolling average is applied to the per minute occupancy count over a custom window size (for our example we have considered 15mins). The python expression used for this is:

sp.signal.savgol_filter(df[“Space-Minute_OccupancyCount”],15,0)

where, savgol_filter is the smoothening filter used from the scipy.signal module which takes in dataframe, and smoothening window and the polynomial order of 0 as the input argument

 Extracting the maximum occupancy count over an hour: From the rolling average calculated in the previous step for the custom window size selected, the maximum occupancy count over an hour is calculated using the expression “x.max()” using the function “AggregateWithExpression”

   
Extracting the maximum occupancy count over a day: From the hourly maximum occupancy count calculated in the previous step for the custom window size selected, the maximum occupancy count over a day is calculated using the expression “x.max()” using the function “AggregateWithExpression”
Step 4:
-	Creation of metrics on the floor level

 Aggregation at floor level: To aggregate at the floor level we use the common output variable “Daily_max_15minwin” set at all the individual spaces and use the function “Sum”.


Step 5: 
-	Creation of metrics on the building level

Aggregation at building level: To aggregate at the building level we use the common output variable “Daily_max_occup_agg” set at all the individual floors and use the function “Sum”.

#### Files in this repository

The repository monitor pipeline utilities consist of different python files:
  
•	pipeline/deploy.py

•	pipeline/dag.py

•	pipeline/util.py

•	pipeline/web.py

•	pipeline/catalog.py

• main.py
  
• requirements.txt

Brief description of the files:

•       catalog.py:

This file consists of transformers and aggregator functions which are used to create rolling averages and weighted averages for the spaces, floors and buildings. It also consists of GET function method which will usually calculate sum, min, max, mean etc functions for the transformation and aggregation. Additionally, there are methods for registering and unregistering aggregator and transformers in the clusters.

•       util.py:

This file is responsible for calling Monitor's REST API.

•       dag.py

This file is used to manage the pipeline in the monitor cluster and consists of different classes which are given below:

◦       class grains: This class is used to manage the granularity such as minutes, hours, days, etc, in the monitor cluster.

◦       metadata: This class if for managing data items, names, types, dimensions, raw metrics and derived metrics.

◦       kpi-tree-node: This class is for managing kpi functions, for getting all the dependencies within the kpi tree and also to get all the descendants within the tree nodes.

◦       class-pipeline-reader: This class is for managing the pipeline reader in the monitor cluster. It also initializes tenant id and even creates the database connection to the cluster. 

•       web.py

This file is used for creating http request and getting http response between client and monitor server.

•       requirements.txt

This file lists two python modules as dependencies

◦       iotfunctions 9.0.0

◦       inquirer 3.2.0

•       deploy.py

This file has three functions to deploy KPI functions to spaces, floors and the building.

•       main.py

This file uses the aforementioned modules for managing the pipeline, deploying spaces and this file usually gets the information of all the buildings in the cluster by running the GET method and based on the command line arguments specified while initializing the function. It allows to select a location interactively to either deploy KPI function or render the pipeline as DAG.

