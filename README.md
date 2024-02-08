# hdx-analysis-scripts

Scripts used by the Centre for Humanitarian Data to analyse and produce statistics on HDX datasets.

These scripts are run manually on a quarterly basis by analysts. They are typically invoked with:

    python get_org_stats.py --output_dir=org_stats

and

    python get_datasets_info.py --output_dir=datasets_info

The runtime for the datasets_info is approximately 5 minutes and generates two CSV files `datasets.csv` which is approximately 13MB and `non_script_updates.csv which is approximately 2Kb.

The runtime for the org_stats is approximately 5 minutes and generates one CSV file `org_stats.csv` which is approximately 36Kb.



## Installation

For local development

Create a virtual environment (assuming Windows for the `activate` command):

```shell
python -m venv venv
source venv/Scripts/activate
```

Then install the requirements:

```shell
pip install -r requirements.txt
pip install -r test-requirements.txt
```

`hdx-analysis-scripts` uses the `hdx-python-api` library, configuration for which is done in the usual way [described here](https://hdx-python-api.readthedocs.io/en/latest/). 

For local use the user agent (`AnalysisScripts`) is specified in the `~/.useragents.yaml` file.
```yaml
hdx-analysis-scripts:
    preprefix: [YOUR_ORGANISATION]
    user_agent: AnalysisScripts
```

These scripts require Mixpanel account credentials to operate, for local development the following environment variables need to be set:

```
MIXPANEL_API_SECRET
MIXPANEL_PROJECT_ID
MIXPANEL_TOKEN
```

Mixpanel accounts for users authenticate by sending a login email to registered users on login request.


