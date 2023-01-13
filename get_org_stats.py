import argparse
import logging
from os import mkdir
from os.path import expanduser, join
from shutil import rmtree

from dateutil.relativedelta import relativedelta
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.organization import Organization
from hdx.facades.keyword_arguments import facade
from hdx.utilities.dateparse import now_utc, parse_date
from hdx.utilities.dictandlist import dict_of_lists_add, write_list_to_csv
from hdx.utilities.downloader import Download

from mixpanel_downloads import get_mixpanel_downloads

logger = logging.getLogger()

lookup = "hdx-analysis-scripts"


def main(output_dir, mixpanel_config_yaml, **ignore):
    rmtree(output_dir, ignore_errors=True)
    mkdir(output_dir)

    configuration = Configuration.read()

    with Download() as downloader:
        today = now_utc()
        last_quarter = today - relativedelta(months=3)
        last_year = today - relativedelta(years=1)
        logger.info("Downloading organisation type lookup")
        url = configuration["org_stats_url"]
        lookups = downloader.download_tabular_cols_as_dicts(url)
        name_to_type = lookups["Org type"]
        logger.info("Downloading links to data explorers and grids")
        json = downloader.download_json(
            "https://data.humdata.org/api/action/hdx_package_links_settings_show"
        )
        dataset_name_to_explorers = dict()
        for explorergridlink in json["result"]:
            explorergrid = explorergridlink["title"]
            for dataset_name in set(explorergridlink["package_list"].split(",")):
                dict_of_lists_add(dataset_name_to_explorers, dataset_name, explorergrid)
        organisations = dict()
        logger.info("Getting all organisation names")
        organisation_names = Organization.get_all_organization_names()
        logger.info("Getting downloads from MixPanel")
        dataset_downloads = get_mixpanel_downloads(
            mixpanel_config_yaml, last_year, today
        )
        logger.info("Obtaining organisations data")
        for organisation_name in organisation_names:
            organisation = Organization.read_from_hdx(organisation_name)
            organisations[organisation_name] = organisation
            organisation_type = name_to_type.get(organisation_name, "")
            organisation["orgtype"] = organisation_type
            organisation["downloads all time"] = 0
            organisation["downloads last year"] = 0
            organisation["datasets"] = 0
            organisation["Updated last 3 months"] = "No"
            organisation["In explorer or grid"] = "No"
        logger.info("Examining all datasets")
        for dataset in Dataset.get_all_datasets():
            if dataset["private"]:
                continue
            organisation_name = dataset["organization"]["name"]
            organisation = organisations[organisation_name]
            downloads_all_time = dataset["total_res_downloads"]
            organisation["downloads all time"] += downloads_all_time
            downloads_last_year = dataset_downloads.get(dataset["id"], 0)
            organisation["downloads last year"] += downloads_last_year
            organisation["datasets"] += 1
            data_updated = parse_date(dataset["last_modified"])
            if data_updated > last_quarter and data_updated <= today:
                organisation["Updated last 3 months"] = "Yes"
            if dataset["name"] in dataset_name_to_explorers:
                organisation["In explorer or grid"] = "Yes"
        headers = [
            "Organisation name",
            "Organisation title",
            "Org type",
            "Downloads all time",
            "Downloads last year",
            "Datasets",
            "Followers",
            "Updated last 3 months",
            "In Explorer or Grid",
        ]
        logger.info("Generating rows")
        rows = list()
        for organisation_name in sorted(organisations):
            organisation = organisations[organisation_name]
            row = [
                organisation_name,
                organisation["title"],
                organisation["orgtype"],
                organisation["downloads all time"],
                organisation["downloads last year"],
                organisation["datasets"],
                organisation["num_followers"],
                organisation["Updated last 3 months"],
                organisation["In explorer or grid"],
            ]
            rows.append(row)
        if rows:
            filepath = join(output_dir, "org_stats.csv")
            logger.info(f"Writing rows to {filepath}")
            write_list_to_csv(filepath, rows, headers)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Org Stats script")
    parser.add_argument("-od", "--output_dir", default="output", help="Output folder")
    args = parser.parse_args()
    home_folder = expanduser("~")
    facade(
        main,
        hdx_read_only=True,
        hdx_site="prod",
        user_agent_config_yaml=join(home_folder, ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
        output_dir=args.output_dir,
        mixpanel_config_yaml=join(home_folder, ".mixpanel.yml"),
    )
