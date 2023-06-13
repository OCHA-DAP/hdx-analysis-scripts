import argparse
import logging
import re
from datetime import timedelta
from os import mkdir
from os.path import expanduser, join
from shutil import rmtree

from dateutil.parser import ParserError
from dateutil.relativedelta import relativedelta
from downloads import Downloads
from hdx.api.configuration import Configuration
from hdx.facades.keyword_arguments import facade
from hdx.utilities.dateparse import default_date, now_utc, parse_date
from hdx.utilities.dictandlist import dict_of_lists_add, write_list_to_csv
from hdx.utilities.text import get_fraction_str

logger = logging.getLogger(__name__)

lookup = "hdx-analysis-scripts"

bracketed_date = re.compile(r"\((.*)\)")


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
        organisation["updated by cod script"] = 0
        organisation["updated by script"] = 0
        organisation["old updated by script"] = 0
        organisation["any updated last 3 months"] = "No"
        organisation["any public updated last 3 months"] = "No"
        organisation["public live datasets"] = 0
        organisation["public ongoing datasets"] = 0
        organisation["latest scripted update date"] = default_date
        organisation["in explorer or grid"] = "No"
    outdated_lastmodifieds = {}
    for dataset in downloads.get_all_datasets():
        name = dataset["name"]
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
            logger.error(f"Dataset {name} has no last modified field!")
            continue
        data_updated = parse_date(data_updated, include_microseconds=True)
        if data_updated > last_quarter and data_updated <= downloads.today:
            organisation["any updated last 3 months"] = "Yes"
            if is_public:
                organisation["any public updated last 3 months"] = "Yes"
        if is_public:
            update_frequency = dataset.get_expected_update_frequency()
            if update_frequency == "Live":
                organisation["public live datasets"] += 1
            reference_period = dataset.get_reference_period()
            if reference_period["ongoing"]:
                organisation["public ongoing datasets"] += 1
        if name in dataset_name_to_explorers:
            organisation["in explorer or grid"] = "Yes"
        updated_by_script = dataset.get("updated_by_script")
        if updated_by_script:
            if data_updated > organisation["latest scripted update date"]:
                organisation["latest scripted update date"] = data_updated
            if is_public:
                if "HDXINTERNAL" in updated_by_script:
                    if any(x in updated_by_script for x in ("tagbot",)):
                        continue
                if any(
                    x in updated_by_script
                    for x in (
                        "HDXPythonLibrary/5.4.8-test (2022-01-04",
                        "HDXPythonLibrary/5.4.1-test (2021-11-17",
                    )
                ):  # Mike maintainer bulk change
                    continue
                if (
                    "HDXINTERNAL" in updated_by_script
                    and "CODs" in updated_by_script
                    and "cod_level" in dataset
                ):
                    organisation["updated by cod script"] += 1
                    continue
                match = bracketed_date.search(updated_by_script)
                if match is None:
                    continue
                else:
                    try:
                        updated_by_script = parse_date(
                            match.group(1), include_microseconds=True
                        )
                        if updated_by_script > data_updated:
                            organisation["updated by script"] += 1
                            difference = updated_by_script - data_updated
                            if difference > timedelta(hours=1):
                                dict_of_lists_add(
                                    outdated_lastmodifieds, organisation_name, name
                                )
                            continue
                        difference = data_updated - updated_by_script
                        if difference < timedelta(hours=1):
                            organisation["updated by script"] += 1
                        else:
                            organisation["old updated by script"] += 1
                    except ParserError:
                        continue

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
        "% of public cod scripted",
        "% of public non-cod scripted",
        "% of public previous scripted",
        "% of public live",
        "% of public ongoing",
        "Followers",
        "Any updated last 3 months",
        "Any public updated last 3 months",
        "Latest scripted update date",
        "In explorer or grid",
    ]
    logger.info("Generating rows")
    rows = list()
    for organisation_name in sorted(organisations):
        organisation = organisations[organisation_name]
        percentage_cod = get_fraction_str(
            organisation["updated by cod script"] * 100,
            organisation["public datasets"],
            format="%.0f",
        )
        percentage_api = get_fraction_str(
            organisation["updated by script"] * 100,
            organisation["public datasets"],
            format="%.0f",
        )
        percentage_old_api = get_fraction_str(
            organisation["old updated by script"] * 100,
            organisation["public datasets"],
            format="%.0f",
        )
        percentage_live = get_fraction_str(
            organisation["public live datasets"] * 100,
            organisation["public datasets"],
            format="%.0f",
        )
        percentage_ongoing = get_fraction_str(
            organisation["public ongoing datasets"] * 100,
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
            percentage_cod,
            percentage_api,
            percentage_old_api,
            percentage_live,
            percentage_ongoing,
            organisation["num_followers"],
            organisation["any updated last 3 months"],
            organisation["any public updated last 3 months"],
            latest_scripted_update_date,
            organisation["in explorer or grid"],
        ]
        rows.append(row)
    if rows:
        filepath = join(output_dir, "org_stats.csv")
        logger.info(f"Writing rows to {filepath}")
        write_list_to_csv(filepath, rows, headers)

    if outdated_lastmodifieds:
        message = ["updated_by_script is significantly after last_modified for:\n"]
        for organisation_name, dataset_names in outdated_lastmodifieds.items():
            message.append(f"organisation {organisation_name} with ")
            no_names = len(dataset_names)
            if no_names > 6:
                message.append(f"{no_names} datasets such as {dataset_names[0]}")
            else:
                message.append("datasets: ")
                for dataset_name in dataset_names:
                    message.append(f"{dataset_name} ")
            message.append("\n")
        logger.warning("".join(message))


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
