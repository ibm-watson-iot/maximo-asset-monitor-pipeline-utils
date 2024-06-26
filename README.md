## monitor-pipeline-utils

Monitor-pipeline-utils is a python module to deal with various use cases:

* Render an asset's pipeline as directed acyclic graph (DAG)
* Create pipeline elements for space planning of a building

### Examples

#### DAG rendering

`python main.py --tenant-id main --site IBMCLOUD_TEST3 --render Green`

Select a location in site IBMCLOUD_TEST3 from all locations with "Green" as substring and their sublocations and render its pipeline as DAG

`python main.py --tenant-id main --site IBMCLOUD_TEST3 --render 101 --td`

Same as above but select locations with "101" as substring and render the DAG top-down instead of left-right

#### Deploy space planning elements

`python main.py --tenant-id main --site IBMCLOUD_TEST3 --deploy Green`

Deploys all KPI functions related to space planning to all locations, spaces and floors, of a building.
The command builds a selection list for all locations with "Green" in its name in site IBMCLOUD_TEST3

### Prerequisites

In order to use the script you need the credentials of the Monitor instance as json document.

Example

```
{
    "baseurl": "https://main.api.monitor.monitortest.ibmmasmonitor.com",
    "apikey": "<APIKEY>",
    "apitoken": "<APITOKEN>"
}
```


