import argparse
import logging
import re
import os
from os import mkdir
from os.path import expanduser, join
from shutil import rmtree

from common import get_dataset_name_to_explorers, get_freshness_by_frequency, \
    get_dataset_id_to_requests
from common.dataset_statistics import DatasetStatistics
from common.downloads import Downloads
from hdx.api.configuration import Configuration
from hdx.facades.keyword_arguments import facade
from hdx.utilities.dateparse import default_date, now_utc
from hdx.utilities.dictandlist import dict_of_lists_add, write_list_to_csv
from hdx.utilities.text import get_fraction_str

logger = logging.getLogger(__name__)

lookup = "hdx-analysis-scripts"

bracketed_date = re.compile(r"\((.*)\)")


def main(downloads, output_dir, **ignore):
    rmtree(output_dir, ignore_errors=True)
    mkdir(output_dir)

    configuration = Configuration.read()

    downloads.set_api_key(configuration.get_api_key())
    org_stats_url = configuration["org_stats_url"]
    name_to_type = downloads.get_org_types(org_stats_url)
    dataset_name_to_explorers = get_dataset_name_to_explorers(downloads)
    dataset_id_to_requests = get_dataset_id_to_requests(downloads)
    freshness_by_frequency = get_freshness_by_frequency(
        downloads, configuration["aging_url"]
    )
    dataset_downloads = downloads.get_mixpanel_downloads(1)
    logger.info("Obtaining organisations data")
    organisations = downloads.get_all_organisations()
    total_public = 0
    total_updated_by_cod = 0
    total_updated_by_script = 0
    for organisation_name, organisation in organisations.items():
        organisation_type = name_to_type.get(organisation_name, "")
        organisation["orgtype"] = organisation_type
        admins = 0
        for user in organisation.get("users", []):
            if user["capacity"] == "admin":
                admins += 1
        organisation["number of admins"] = admins
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
        organisation["any updated previous quarter"] = "No"
        organisation["any public updated previous quarter"] = "No"
        organisation["public live datasets"] = 0
        organisation["public ongoing datasets"] = 0
        organisation["latest scripted update date"] = default_date
        organisation["in explorer or grid"] = "No"
        organisation["marked inactive"] = (
            "Yes" if organisation.get("closed_organization", False) else "No"
        )
        organisation["tags"] = set()
        organisation["new requests"] = 0
        organisation["open requests"] = 0
        organisation["shared requests"] = 0
        organisation["rejected requests"] = 0
    outdated_lastmodifieds = {}
    for dataset in downloads.get_all_datasets():
        datasetstats = DatasetStatistics(
            downloads.today, dataset_name_to_explorers, dataset_id_to_requests,
            freshness_by_frequency,
            dataset
        )
        name = dataset["name"]
        organisation_name = dataset["organization"]["name"]
        organisation = organisations[organisation_name]
        is_public_not_requestable_archived = False
        if datasetstats.public == "N":
            organisation["private datasets"] += 1
            continue
        elif datasetstats.requestable == "Y":
            organisation["requestable datasets"] += 1
        elif datasetstats.archived == "Y":
            organisation["archived datasets"] += 1
        else:
            organisation["public datasets"] += 1
            total_public += 1
            is_public_not_requestable_archived = True

        downloads_all_time = dataset["total_res_downloads"]
        organisation["downloads all time"] += downloads_all_time
        downloads_last_year = dataset_downloads.get(dataset["id"], 0)
        organisation["downloads last year"] += downloads_last_year
        if datasetstats.last_modified is None:
            continue
        if datasetstats.updated_last_3_months == "Y":
            organisation["any updated last 3 months"] = "Yes"
            if is_public_not_requestable_archived:
                organisation["any public updated last 3 months"] = "Yes"
        if datasetstats.updated_previous_qtr == "Y":
            organisation["any updated previous quarter"] = "Yes"
            if is_public_not_requestable_archived:
                organisation["any public updated previous quarter"] = "Yes"
        if is_public_not_requestable_archived:
            if datasetstats.live == "Y":
                organisation["public live datasets"] += 1
            if datasetstats.ongoing == "Y":
                organisation["public ongoing datasets"] += 1
        if datasetstats.in_explorer_or_grid == "Y":
            organisation["in explorer or grid"] = "Yes"
        if datasetstats.updated_by_cod_script == "Y":
            organisation["updated by cod script"] += 1
            total_updated_by_cod += 1
        organisation["new requests"] += datasetstats.new_requests
        organisation["open requests"] += datasetstats.open_requests
        organisation["shared requests"] += datasetstats.shared_requests
        organisation["rejected requests"] += datasetstats.rejected_requests
        if datasetstats.updated_by_script:
            if datasetstats.last_modified > organisation[
                "latest scripted update date"]:
                organisation[
                    "latest scripted update date"] = datasetstats.last_modified
            if datasetstats.updated_by_noncod_script == "Y":
                organisation["updated by script"] += 1
                total_updated_by_script += 1
            if datasetstats.outdated_lastmodified == "Y":
                dict_of_lists_add(outdated_lastmodifieds, organisation_name,
                                  name)
            if datasetstats.old_updated_by_noncod_script == "Y":
                organisation["old updated by script"] += 1
        datasetstats.add_tags_to_set(organisation["tags"])

    headers = [
        "Organisation name",
        "Organisation title",
        "Organisation acronym",
        "Org type",
        "Number of admins",
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
        "Any updated previous quarter",
        "Any public updated previous quarter",
        "Latest scripted update date",
        "In explorer or grid",
        "Marked inactive",
        "New requests",
        "Open requests",
        "Shared requests",
        "Rejected requests",
        "Tags",
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

        latest_scripted_update_date = organisation[
            "latest scripted update date"]
        if latest_scripted_update_date == default_date:
            latest_scripted_update_date = None
        else:
            latest_scripted_update_date = latest_scripted_update_date.date().isoformat()
        row = [
            organisation_name,
            organisation["title"],
            organisation.get("org_acronym", ""),
            organisation["orgtype"],
            organisation["number of admins"],
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
            organisation["any updated previous quarter"],
            organisation["any public updated previous quarter"],
            latest_scripted_update_date,
            organisation["in explorer or grid"],
            organisation["marked inactive"],
            organisation["new requests"],
            organisation["open requests"],
            organisation["shared requests"],
            organisation["rejected requests"],
            ",".join(sorted(organisation["tags"])),
        ]
        rows.append(row)
    if rows:
        filepath = join(output_dir, "org_stats.csv")
        logger.info(f"Writing rows to {filepath}")
        write_list_to_csv(filepath, rows, headers, encoding="utf-8")

    if outdated_lastmodifieds:
        message = [
            "updated_by_script is significantly after last_modified for:\n"]
        for organisation_name, dataset_names in outdated_lastmodifieds.items():
            message.append(f"organisation {organisation_name} with ")
            no_names = len(dataset_names)
            if no_names > 6:
                message.append(
                    f"{no_names} datasets such as {dataset_names[0]}")
            else:
                message.append("datasets: ")
                for dataset_name in dataset_names:
                    message.append(f"{dataset_name} ")
            message.append("\n")
        logger.warning("".join(message))

    logger.info(f"Total public datasets = {total_public}")
    logger.info(f"Total public updated by cod script = {total_updated_by_cod}")
    logger.info(
        f"Total public updated by all other scripts = {total_updated_by_script}"
    )
    return total_public, total_updated_by_cod, total_updated_by_script


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Org Stats script")
    parser.add_argument("-od", "--output_dir", default="output",
                        help="Output folder")
    parser.add_argument(
        "-sd", "--saved_dir", default=None, help="Dir for downloaded data"
    )
    args = parser.parse_args()
    home_folder = expanduser("~")
    today = now_utc()
    mixpanel_config_yaml = join(home_folder, ".mixpanel.yml")
    downloads = Downloads(today, mixpanel_config_yaml, args.saved_dir)

    user_agent_config_path = join(home_folder, ".useragents.yaml")
    if not os.path.exists(user_agent_config_path):
        user_agent_config_path = join(home_folder, ".useragents.yml")
    facade(
        main,
        hdx_site="prod",
        user_agent_config_yaml=user_agent_config_path,
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
        downloads=downloads,
        output_dir=args.output_dir,
    )
