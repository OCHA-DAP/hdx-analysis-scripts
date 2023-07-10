import logging
from os import getenv
from os.path import join

from dateutil.relativedelta import relativedelta
from hdx.data.dataset import Dataset
from hdx.data.organization import Organization
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.downloader import Download
from hdx.utilities.loader import load_yaml
from hdx.utilities.saver import save_json
from mixpanel_utils import MixpanelUtils

logger = logging.getLogger(__name__)


class Downloads:
    mixpanel_file = "mixpanel.json"
    datasets_file = "datasets.json"
    orgtypes_file = "org_types.json"
    packagelinks_file = "package_links.json"
    organisations_file = "organisations.json"

    def __init__(self, today, mixpanel_config_yaml, saved_dir=None):
        self.today = today
        self.mixpanel_config_yaml = mixpanel_config_yaml
        self.saved_dir = saved_dir

    def get_mixpanel_downloads(self, years_ago):
        end_date = self.today
        start_date = end_date - relativedelta(years=years_ago)
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
        jql_query = """
        function main() {
          return Events({
            from_date: '%s',
            to_date: '%s',
            event_selectors: [{event: "resource download"}]
          })
          .groupByUser(["properties.resource id","properties.dataset id",mixpanel.numeric_bucket('time',mixpanel.daily_time_buckets)],mixpanel.reducer.null())
          .groupBy(["key.2"], mixpanel.reducer.count())
            .map(function(r){
            return [
              r.key[0], r.value
            ];
          });
        }""" % (
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
        datasets = Dataset.get_all_datasets()
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

    def get_org_types(self, url):
        logger.info("Downloading organisation type lookup")
        lookups = Download().download_tabular_cols_as_dicts(url)
        orgs_types = lookups["Org type"]
        if self.saved_dir:
            save_json(orgs_types, join(self.saved_dir, self.orgtypes_file))
        return orgs_types

    def get_package_links(self):
        logger.info("Downloading links to data explorers and grids")
        json = Download().download_json(
            "https://data.humdata.org/api/action/hdx_package_links_settings_show"
        )
        if self.saved_dir:
            save_json(json, join(self.saved_dir, self.packagelinks_file))
        return json

    def get_all_organisations(self):
        logger.info("Obtaining organisations data")
        organisation_names = Organization.get_all_organization_names()
        organisations = dict()
        for organisation_name in organisation_names:
            organisation = Organization.read_from_hdx(organisation_name)
            organisations[organisation_name] = organisation.data
        if self.saved_dir:
            save_json(organisations, join(self.saved_dir, self.organisations_file))
        return organisations
