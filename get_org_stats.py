import argparse
import logging
import re
import os
from os import mkdir
from os.path import expanduser, join
from shutil import rmtree

from hdx.location.country import Country

from common import get_dataset_name_to_explorers, get_aging, get_requests_mappings
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
    org_type_mapping = configuration["org_type_mapping"]
    org_stats_url = configuration["org_stats_url"]
    name_to_geospatiality, name_to_location = downloads.get_geospatiality_locations(
        org_stats_url
    )
    dataset_name_to_explorers = get_dataset_name_to_explorers(downloads)
    dataset_id_to_requests, organisation_name_to_requests = get_requests_mappings(
        downloads
    )
    last_modified_aging = get_aging(configuration["last_modified_aging"])
    end_date_aging = get_aging(configuration["end_date_aging"])
    dataset_3m_downloads = downloads.get_mixpanel_downloads(3)
    dataset_1y_downloads = downloads.get_mixpanel_downloads(12)
    logger.info("Obtaining organisations data")
    organisations = downloads.get_all_organisations()
    total_public = 0
    total_public_internal = 0
    total_public_external = 0
    total_updated_by_cod = 0
    total_updated_by_script = 0
    total_lm_fresh = 0
    total_lm_not_fresh = 0
    total_ed_uptodate = 0
    total_ed_outofdate = 0
    for organisation_name, organisation in organisations.items():
        geospatiality = name_to_geospatiality.get(organisation_name, "")
        organisation["geospatiality"] = geospatiality
        organisation_location = name_to_location.get(organisation_name, "")
        organisation["location"] = organisation_location
        latitude, longitude = "", ""
        if organisation_location and len(organisation_location) == 3:
            country_info = Country.get_country_info_from_iso3(organisation_location)
            if country_info:
                latitude = country_info["#geo+lat"]
                longitude = country_info["#geo+lon"]
        organisation["latitude"] = latitude
        organisation["longitude"] = longitude
        admins = 0
        editors = 0
        members = 0
        for user in organisation["users"]:
            match user["capacity"]:
                case "admin":
                    admins += 1
                case "editor":
                    editors += 1
                case "member":
                    members += 1
                case x:
                    raise ValueError(f"Unknown capacity {x}!")
        organisation["number of admins"] = admins
        organisation["number of editors"] = editors
        organisation["number of members"] = members
        organisation["downloads last 90 days"] = 0
        organisation["downloads last 12 months"] = 0
        organisation["public datasets"] = 0
        organisation["requestable datasets"] = 0
        organisation["private datasets"] = 0
        organisation["archived datasets"] = 0
        organisation["public internal resources"] = 0
        organisation["public external resources"] = 0
        organisation["updated by cod script"] = 0
        organisation["formerly updated by cod script"] = 0
        organisation["updated by script"] = 0
        organisation["old updated by script"] = 0
        organisation["any updated last 3 months"] = "No"
        organisation["any public updated last 3 months"] = "No"
        organisation["any updated previous quarter"] = "No"
        organisation["any public updated previous quarter"] = "No"
        organisation["public live datasets"] = 0
        organisation["public ongoing datasets"] = 0
        organisation["lm fresh datasets"] = 0
        organisation["lm due datasets"] = 0
        organisation["lm overdue datasets"] = 0
        organisation["lm delinquent datasets"] = 0
        organisation["ed uptodate datasets"] = 0
        organisation["ed outofdate datasets"] = 0
        organisation["latest created dataset date"] = default_date
        organisation["latest scripted update date"] = default_date
        organisation["in explorer or grid"] = "No"
        organisation["closed"] = "Yes" if organisation["closed_organization"] else "No"

        new_requests = 0
        open_requests = 0
        archived_requests = 0
        shared_requests = 0
        denied_requests = 0
        for request in organisation_name_to_requests.get(organisation_name, []):
            if request["state"] == "new":
                new_requests += 1
            elif request["state"] == "open":
                open_requests += 1
            else:
                archived_requests += 1
                if request["data_shared"]:
                    shared_requests += 1
                elif request["rejected"]:
                    denied_requests += 1
        organisation["new requests"] = new_requests
        organisation["open requests"] = open_requests
        organisation["archived requests"] = archived_requests
        organisation["shared requests"] = shared_requests
        organisation["denied requests"] = denied_requests
        organisation["tags"] = set()
        organisation["has crisis"] = "N"
    outdated_lastmodifieds = {}
    for dataset in downloads.get_all_datasets():
        datasetstats = DatasetStatistics(
            downloads.today,
            dataset_name_to_explorers,
            dataset_id_to_requests,
            last_modified_aging,
            end_date_aging,
            dataset,
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
            organisation["public internal resources"] += datasetstats.internal_resources
            organisation["public external resources"] += datasetstats.external_resources
            total_public_internal += datasetstats.internal_resources
            total_public_external += datasetstats.external_resources

        downloads_last_3months = dataset_3m_downloads.get(dataset["id"], 0)
        organisation["downloads last 90 days"] += downloads_last_3months
        downloads_last_year = dataset_1y_downloads.get(dataset["id"], 0)
        organisation["downloads last 12 months"] += downloads_last_year
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
        match datasetstats.last_modified_fresh:
            case "Fresh":
                organisation["lm fresh datasets"] += 1
                total_lm_fresh += 1
            case "Due":
                organisation["lm due datasets"] += 1
                total_lm_not_fresh += 1
            case "Overdue":
                organisation["lm overdue datasets"] += 1
                total_lm_not_fresh += 1
            case "Delinquent":
                organisation["lm delinquent datasets"] += 1
                total_lm_not_fresh += 1
        match datasetstats.end_date_uptodate:
            case "UpToDate":
                organisation["ed uptodate datasets"] += 1
                total_ed_uptodate += 1
            case "OutOfDate":
                organisation["ed outofdate datasets"] += 1
                total_ed_outofdate += 1
        if datasetstats.in_explorer_or_grid == "Y":
            organisation["in explorer or grid"] = "Yes"
        if (
            datasetstats.updated_by_cod_script == "Y"
            and is_public_not_requestable_archived
        ):
            organisation["updated by cod script"] += 1
            total_updated_by_cod += 1
        if (
            datasetstats.old_updated_by_cod_script == "Y"
            and is_public_not_requestable_archived
        ):
            organisation["formerly updated by cod script"] += 1
            total_updated_by_cod += 1
        if datasetstats.created > organisation["latest created dataset date"]:
            organisation["latest created dataset date"] = datasetstats.created
        if datasetstats.updated_by_script:
            if datasetstats.last_modified > organisation["latest scripted update date"]:
                organisation["latest scripted update date"] = datasetstats.last_modified
            if (
                datasetstats.updated_by_noncod_script == "Y"
                and is_public_not_requestable_archived
            ):
                organisation["updated by script"] += 1
                total_updated_by_script += 1
            if datasetstats.outdated_lastmodified == "Y":
                dict_of_lists_add(outdated_lastmodifieds, organisation_name, name)
            if datasetstats.old_updated_by_noncod_script == "Y":
                organisation["old updated by script"] += 1
        datasetstats.add_tags_to_set(organisation["tags"])
        if datasetstats.crisis_tag:
            organisation["has crisis"] = "Y"

    headers = [
        "Organisation name",
        "Organisation title",
        "Organisation acronym",
        "Organisation id",
        "Organisation type",
        "Geospatiality",
        "Location",
        "Latitude",
        "Longitude",
        "Number of admins",
        "Number of editors",
        "Number of members",
        "Downloads last 90 days",
        "Downloads last 12 months",
        "Public datasets",
        "Requestable datasets",
        "Private datasets",
        "Archived datasets",
        "Public Internal Resources",
        "Public External Resources",
        "Public API (non-cod scripted)",
        "% of public API (non-cod scripted)",
        "Public cod scripted",
        "% of public cod scripted",
        "Public formerly cod scripted",
        "% of public formerly cod scripted",
        "Public previous scripted",
        "% of public previous scripted",
        "Public live",
        "% of public live",
        "Public ongoing",
        "% of public ongoing",
        "Followers",
        "Any updated last 3 months",
        "Any public updated last 3 months",
        "Any updated previous quarter",
        "Any public updated previous quarter",
        "Last modified fresh datasets",
        "Last modified due datasets",
        "Last modified overdue datasets",
        "Last modified delinquent datasets",
        "End date up to date datasets",
        "End date out of date datasets",
        "Latest created dataset date",
        "Latest scripted update date",
        "In explorer or grid",
        "Closed",
        "New requests",
        "Open requests",
        "Total archived requests",
        "Shared requests",
        "Denied requests",
        "Tags",
        "Has crisis",
    ]

    def get_number_percentage(organisation, key):
        number = organisation[key]
        if number == "":
            return "", ""
        percentage = get_fraction_str(
            number * 100,
            organisation["public datasets"],
            format="%.0f",
        )
        return number, percentage

    logger.info("Generating rows")
    rows = list()
    for organisation_name in sorted(organisations):
        organisation = organisations[organisation_name]
        organisation_type = org_type_mapping[organisation["hdx_org_type"]]
        updated_by_cod_script, percentage_cod = get_number_percentage(
            organisation, "updated by cod script"
        )
        old_updated_by_cod_script, old_percentage_cod = get_number_percentage(
            organisation, "formerly updated by cod script"
        )
        updated_by_api, percentage_api = get_number_percentage(
            organisation, "updated by script"
        )
        old_updated_by_script, percentage_old_script = get_number_percentage(
            organisation, "old updated by script"
        )
        live_datasets, percentage_live = get_number_percentage(
            organisation, "public live datasets"
        )
        ongoing_datasets, percentage_ongoing = get_number_percentage(
            organisation, "public ongoing datasets"
        )

        latest_created_dataset_date = organisation["latest created dataset date"]
        if latest_created_dataset_date == default_date:
            latest_created_dataset_date = None
        else:
            latest_created_dataset_date = latest_created_dataset_date.date().isoformat()
        latest_scripted_update_date = organisation["latest scripted update date"]
        if latest_scripted_update_date == default_date:
            latest_scripted_update_date = None
        else:
            latest_scripted_update_date = latest_scripted_update_date.date().isoformat()
        row = [
            organisation_name,
            organisation["title"],
            organisation.get("org_acronym", ""),
            organisation["id"],
            organisation_type,
            organisation["geospatiality"],
            organisation["location"],
            organisation["latitude"],
            organisation["longitude"],
            organisation["number of admins"],
            organisation["number of editors"],
            organisation["number of members"],
            organisation["downloads last 90 days"],
            organisation["downloads last 12 months"],
            organisation["public datasets"],
            organisation["requestable datasets"],
            organisation["private datasets"],
            organisation["archived datasets"],
            organisation["public internal resources"],
            organisation["public external resources"],
            updated_by_api,
            percentage_api,
            updated_by_cod_script,
            percentage_cod,
            old_updated_by_cod_script,
            old_percentage_cod,
            old_updated_by_script,
            percentage_old_script,
            live_datasets,
            percentage_live,
            ongoing_datasets,
            percentage_ongoing,
            organisation["num_followers"],
            organisation["any updated last 3 months"],
            organisation["any public updated last 3 months"],
            organisation["any updated previous quarter"],
            organisation["any public updated previous quarter"],
            organisation["lm fresh datasets"],
            organisation["lm due datasets"],
            organisation["lm overdue datasets"],
            organisation["lm delinquent datasets"],
            organisation["ed uptodate datasets"],
            organisation["ed outofdate datasets"],
            latest_created_dataset_date,
            latest_scripted_update_date,
            organisation["in explorer or grid"],
            organisation["closed"],
            organisation["new requests"],
            organisation["open requests"],
            organisation["archived requests"],
            organisation["shared requests"],
            organisation["denied requests"],
            ",".join(sorted(organisation["tags"])),
            organisation["has crisis"],
        ]
        rows.append(row)
    if rows:
        filepath = join(output_dir, "org_stats.csv")
        logger.info(f"Writing rows to {filepath}")
        write_list_to_csv(filepath, rows, headers, encoding="utf-8")

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

    logger.info(
        f"Total public datasets (excluding requestable, archived) = {total_public}"
    )
    logger.info(f"Total public updated by cod script = {total_updated_by_cod}")
    logger.info(
        f"Total public updated by all other scripts = {total_updated_by_script}"
    )
    quarterly_api_okr = get_fraction_str(
        total_updated_by_script * 100,
        total_public,
        format="%.0f",
    )
    logger.info(f"Quarterly % API OKR = {quarterly_api_okr}")

    logger.info(f"Total fresh datasets (using last modified) = {total_lm_fresh}")
    logger.info(
        f"Total non-fresh datasets (using last modified) = {total_lm_not_fresh}"
    )
    quarterly_lm_fresh_okr = get_fraction_str(
        total_lm_fresh * 100,
        (total_lm_fresh + total_lm_not_fresh),
        format="%.0f",
    )
    logger.info(f"Quarterly % last modified fresh OKR = {quarterly_lm_fresh_okr}")

    logger.info(f"Total up to date datasets (using end date) = {total_ed_uptodate}")
    logger.info(f"Total out of date datasets (using end date) = {total_ed_outofdate}")
    quarterly_ed_uptodate_okr = get_fraction_str(
        total_ed_uptodate * 100,
        (total_ed_uptodate + total_ed_outofdate),
        format="%.0f",
    )
    logger.info(f"Quarterly % end date up to date OKR = {quarterly_ed_uptodate_okr}")
    filepath = join(output_dir, "total_stats.csv")
    logger.info(f"Writing totals to {filepath}")
    headers = [
        "Public - Request & Archive",
        "Public Internal Resources",
        "Public External Resources",
        "Updated by COD",
        "Updated by Script",
        "Quarterly % API OKR",
        "Last Modified Fresh",
        "Last Modified Not Fresh",
        "Quarterly % Last Modified Fresh OKR",
        "End Date Up to Date",
        "End Date Out Of Date",
        "Quarterly % End Date Up To Date OKR",
    ]
    rows = [
        [
            total_public,
            total_public_internal,
            total_public_external,
            total_updated_by_cod,
            total_updated_by_script,
            quarterly_api_okr,
            total_lm_fresh,
            total_lm_not_fresh,
            quarterly_lm_fresh_okr,
            total_ed_uptodate,
            total_ed_outofdate,
            quarterly_ed_uptodate_okr,
        ]
    ]
    write_list_to_csv(filepath, rows, headers, encoding="utf-8")
    return total_public, total_updated_by_cod, total_updated_by_script


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Org Stats script")
    parser.add_argument("-od", "--output_dir", default="output", help="Output folder")
    parser.add_argument(
        "-sd", "--saved_dir", default=None, help="Dir for downloaded data"
    )
    args = parser.parse_args()
    home_folder = expanduser("~")
    today = now_utc()
    mixpanel_config_yaml = join(home_folder, ".mixpanel.yaml")
    downloads = Downloads(today, mixpanel_config_yaml, args.saved_dir)

    user_agent_config_path = join(home_folder, ".useragents.yaml")
    if not os.path.exists(user_agent_config_path):
        user_agent_config_path = join(home_folder, ".useragents.yml")
    facade(
        main,
        hdx_site="prod",
        user_agent_config_yaml=user_agent_config_path,
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yaml"),
        downloads=downloads,
        output_dir=args.output_dir,
    )
