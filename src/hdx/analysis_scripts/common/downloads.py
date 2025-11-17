import logging
from os import getenv
from os.path import join

from dateutil.relativedelta import relativedelta
from mixpanel_utils import MixpanelUtils

from hdx.data.dataset import Dataset
from hdx.data.organization import Organization
from hdx.data.user import User
from hdx.utilities.downloader import Download
from hdx.utilities.loader import load_yaml
from hdx.utilities.saver import save_json

logger = logging.getLogger(__name__)

COMMON_HEADER = """
const BOT_STRINGS = ['bot', 'crawler', 'spider', 'hxl-proxy', 'hdxinternal', 'data\.world', 'opendataportalwatch', 'microsoft\ office', 'turnitin']
const regex = new RegExp(BOT_STRINGS.join('|'), 'i')
function containsAny(value) {{
  if (value) {{
    return regex.test(value)
  }}
}}
"""

COMMON_FILTER = """
  .filter(event => event.properties['org name'] && event.properties['org name'] != 'None')
  .filter(event => event.properties['user agent'] != '' && !containsAny(event.properties['user agent']) && !containsAny(event.properties['$browser']))
"""

query_template = (
    COMMON_HEADER
    + """
function main() {{
  return Events({{
    from_date: '{}',
    to_date: '{}',
    event_selectors: [{{event: "resource download"}}]
  }})"""
    + COMMON_FILTER
    + """
  .groupByUser(["properties.resource id","properties.dataset id",mixpanel.numeric_bucket('time',mixpanel.daily_time_buckets)],mixpanel.reducer.null())
  .groupBy(["key.2"], mixpanel.reducer.count())
  .map(function(r){{
    return [
      r.key[0],
      r.value
    ];
  }});
}}
"""
)


class Downloads:
    mixpanel_file = "mixpanel.json"
    datasets_file = "datasets.json"
    geospatiality_file = "geospatiality.json"
    locations_file = "locations.json"
    packagelinks_file = "package_links.json"
    hdxconnect_file = "hdxconnect.json"
    organisations_file = "organisations.json"
    users_file = "users.json"
    aging_file = "aging.yaml"

    def __init__(self, today, mixpanel_config_yaml, saved_dir=None):
        self.today = today
        self.mixpanel_config_yaml = mixpanel_config_yaml
        self.headers = {}
        self.saved_dir = saved_dir

    def set_api_key(self, api_key):
        self.headers = {"Authorization": api_key}

    def get_mixpanel_downloads(self, months_ago):
        end_date = self.today
        start_date = end_date - relativedelta(months=months_ago)
        logger.info("Getting downloads from MixPanel")
        try:
            mixpanel_config = load_yaml(self.mixpanel_config_yaml)
            api_secret = mixpanel_config["api_secret"]
            project_id = mixpanel_config["project_id"]
            token = mixpanel_config["token"]
        except FileNotFoundError:
            api_secret = getenv("MIXPANEL_API_SECRET")
            project_id = getenv("MIXPANEL_PROJECT_ID")
            token = getenv("MIXPANEL_TOKEN")
        mputils = MixpanelUtils(
            api_secret=api_secret,
            project_id=project_id,
            token=token,
        )
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        jql_query = query_template.format(
            start_date_str,
            end_date_str,
        )
        datasets_dict = dict(mputils.query_jql(jql_query))
        if self.saved_dir:
            filename = self.mixpanel_file.replace(
                ".json", f"_{start_date_str}-{end_date_str}.json"
            )
            save_json(datasets_dict, join(self.saved_dir, filename))
        return datasets_dict

    def get_all_datasets(self):
        logger.info("Examining all datasets")
        datasets = Dataset.get_all_datasets(include_private=True)
        if self.saved_dir:
            datasets_list = []
            n = -1

            def save_next():
                nonlocal n

                if n >= 0:
                    filename = self.datasets_file.replace(".json", f"_{n}.json")
                    save_json(datasets_list, join(self.saved_dir, filename))
                    datasets_list.clear()
                n += 1

            for i, dataset in enumerate(datasets):
                if i % 1000 == 0:
                    save_next()
                datasets_list.append(dataset.get_dataset_dict())
            save_next()

        return datasets

    def get_geospatiality_locations(self, url):
        logger.info("Downloading organisation geospatiality and location lookup")
        lookups = Download().download_tabular_cols_as_dicts(url)
        geospatiality = lookups["Geospatiality"]
        locations = lookups["Location (ISO 3)"]
        if self.saved_dir:
            save_json(geospatiality, join(self.saved_dir, self.geospatiality_file))
            save_json(locations, join(self.saved_dir, self.locations_file))
        return geospatiality, locations

    def get_package_links(self):
        logger.info("Downloading links to data explorers and grids")
        json = Download().download_json(
            "https://data.humdata.org/api/action/hdx_package_links_settings_show"
        )
        if self.saved_dir:
            save_json(json, join(self.saved_dir, self.packagelinks_file))
        return json

    def get_requests(self):
        logger.info("Downloading HDX Connect requests")
        json = Download(headers=self.headers).download_json(
            "https://data.humdata.org/ckan-admin/requests_data/download?format=json"
        )
        if self.saved_dir:
            save_json(json, join(self.saved_dir, self.hdxconnect_file))
        return json

    def get_all_organisations(self):
        logger.info("Obtaining organisations data")
        organisation_list = Organization.get_all_organization_names(
            all_fields=True,
            include_extras=True,
            include_users=True,
            include_followers=True,
        )
        organisations = {}
        for organisation in organisation_list:
            organisations[organisation["id"]] = organisation
        if self.saved_dir:
            save_json(organisations, join(self.saved_dir, self.organisations_file))
        return organisations

    def get_all_users(self):
        logger.info("Obtaining user data")
        user_list = User.get_all_users()
        users = {}
        for user in user_list:
            users[user["id"]] = user.data
        if self.saved_dir:
            save_json(users, join(self.saved_dir, self.users_file))
        return users
