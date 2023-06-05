import argparse
import logging
from os import mkdir
from os.path import expanduser, join
from shutil import rmtree

from dateutil.relativedelta import relativedelta
from downloads import Downloads
from hdx.api.configuration import Configuration
from hdx.facades.keyword_arguments import facade
from hdx.utilities.dateparse import default_date, now_utc, parse_date
from hdx.utilities.dictandlist import dict_of_lists_add, write_list_to_csv
from hdx.utilities.text import get_fraction_str

logger = logging.getLogger(__name__)

lookup = "hdx-analysis-scripts"


def main(downloads, output_dir, **ignore):
    rmtree(output_dir, ignore_errors=True)
    mkdir(output_dir)

    configuration = Configuration.read()

    last_quarter = downloads.today - relativedelta(months=3)
    url = configuration["org_stats_url"]
    name_to_type = downloads.get_org_types(url)
    json = downloads.get_package_links()
    dataset_name_to_explorers = dict()
    for explorergridlink in json["result"]:
        explorergrid = explorergridlink["title"]
        for dataset_name in set(explorergridlink["package_list"].split(",")):
            dict_of_lists_add(dataset_name_to_explorers, dataset_name, explorergrid)
    dataset_downloads = downloads.get_mixpanel_downloads(1)
    logger.info("Obtaining organisations data")
    organisations = downloads.get_all_organisations()
    for organisation_name, organisation in organisations.items():
        organisation_type = name_to_type.get(organisation_name, "")
        organisation["orgtype"] = organisation_type
        organisation["downloads all time"] = 0
        organisation["downloads last year"] = 0
        organisation["public datasets"] = 0
        organisation["requestable datasets"] = 0
        organisation["private datasets"] = 0
        organisation["archived datasets"] = 0
        organisation["updated by script"] = 0
        organisation["any updated last 3 months"] = "No"
        organisation["public updated last 3 months"] = "No"
        organisation["latest scripted update date"] = default_date
        organisation["in explorer or grid"] = "No"
    for dataset in downloads.get_all_datasets():
        organisation_name = dataset["organization"]["name"]
        organisation = organisations[organisation_name]
        is_public = False
        if dataset["private"]:
            organisation["private datasets"] += 1
            continue
        elif dataset.is_requestable():
            organisation["requestable datasets"] += 1
        elif dataset["archived"]:
            organisation["archived datasets"] += 1
        else:
            is_public = True
            organisation["public datasets"] += 1
        downloads_all_time = dataset["total_res_downloads"]
        organisation["downloads all time"] += downloads_all_time
        downloads_last_year = dataset_downloads.get(dataset["id"], 0)
        organisation["downloads last year"] += downloads_last_year
        data_updated = dataset.get("last_modified")
        if not data_updated:
            name = dataset["name"]
            logger.error(f"Dataset {name} has no last modified field!")
            continue
        data_updated = parse_date(data_updated)
        if data_updated > last_quarter and data_updated <= downloads.today:
            organisation["any updated last 3 months"] = "Yes"
            if is_public:
                organisation["public updated last 3 months"] = "Yes"
        if dataset["name"] in dataset_name_to_explorers:
            organisation["in explorer or grid"] = "Yes"
        updated_by_script = dataset.get("updated_by_script")
        if updated_by_script:
            if data_updated > organisation["latest scripted update date"]:
                organisation["latest scripted update date"] = data_updated
            if is_public:
                if "tagbot" in updated_by_script and "HDXINTERNAL" in updated_by_script:
                    continue
                organisation["updated by script"] += 1

    headers = [
        "Organisation name",
        "Organisation title",
        "Org type",
        "Downloads all time",
        "Downloads last year",
        "Public datasets",
        "Requestable datasets",
        "Private datasets",
        "Archived datasets",
        "% of public scripted",
        "Followers",
        "Any updated last 3 months",
        "Public updated last 3 months",
        "Latest scripted update date",
        "In explorer or grid",
    ]
    logger.info("Generating rows")
    rows = list()
    for organisation_name in sorted(organisations):
        organisation = organisations[organisation_name]
        percentage_api = get_fraction_str(
            organisation["updated by script"] * 100,
            organisation["public datasets"],
            format="%.0f",
        )
        latest_scripted_update_date = organisation["latest scripted update date"]
        if latest_scripted_update_date == default_date:
            latest_scripted_update_date = None
        else:
            latest_scripted_update_date = latest_scripted_update_date.date().isoformat()
        row = [
            organisation_name,
            organisation["title"],
            organisation["orgtype"],
            organisation["downloads all time"],
            organisation["downloads last year"],
            organisation["public datasets"],
            organisation["requestable datasets"],
            organisation["private datasets"],
            organisation["archived datasets"],
            percentage_api,
            organisation["num_followers"],
            organisation["any updated last 3 months"],
            organisation["public updated last 3 months"],
            latest_scripted_update_date,
            organisation["in explorer or grid"],
        ]
        rows.append(row)
    if rows:
        filepath = join(output_dir, "org_stats.csv")
        logger.info(f"Writing rows to {filepath}")
        write_list_to_csv(filepath, rows, headers)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Org Stats script")
    parser.add_argument("-od", "--output_dir", default="output", help="Output folder")
    parser.add_argument(
        "-sd", "--saved_dir", default=None, help="Dir for downloaded data"
    )
    args = parser.parse_args()
    home_folder = expanduser("~")
    today = now_utc()
    mixpanel_config_yaml = join(home_folder, ".mixpanel.yml")
    downloads = Downloads(today, mixpanel_config_yaml, args.saved_dir)
    facade(
        main,
        hdx_read_only=True,
        hdx_site="prod",
        user_agent_config_yaml=join(home_folder, ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
        downloads=downloads,
        output_dir=args.output_dir,
    )
