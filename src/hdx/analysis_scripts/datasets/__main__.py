import argparse
import logging
import os
from os import mkdir
from os.path import expanduser, join
from shutil import rmtree

from hdx.analysis_scripts.common import (
    get_aging,
    get_dataset_name_to_explorers,
    get_requests_mappings,
)
from hdx.analysis_scripts.common.dataset_statistics import DatasetStatistics
from hdx.analysis_scripts.common.downloads import Downloads
from hdx.api.configuration import Configuration
from hdx.facades.keyword_arguments import facade
from hdx.utilities.dateparse import now_utc
from hdx.utilities.dictandlist import write_list_to_csv
from hdx.utilities.path import script_dir_plus_file

logger = logging.getLogger(__name__)

lookup = "hdx-analysis-scripts"


def main(downloads, output_dir, **ignore):
    rmtree(output_dir, ignore_errors=True)
    mkdir(output_dir)

    configuration = Configuration.read()
    downloads.set_api_key(configuration.get_api_key())

    dataset_name_to_explorers = get_dataset_name_to_explorers(downloads)
    dataset_id_to_requests, _ = get_requests_mappings(downloads)
    last_modified_aging = get_aging(configuration["last_modified_aging"])
    end_date_aging = get_aging(configuration["end_date_aging"])
    dataset_downloads = downloads.get_mixpanel_downloads(60)
    created_per_month = {}
    metadata_updated_per_month = {}
    data_updated_per_month = {}
    rows = [
        (
            "name",
            "title",
            "id",
            "downloads last 5 years",
            "date created",
            "date metadata updated",
            "date data updated",
            "updated last 3 months",
            "updated previous quarter",
            "reference period start",
            "reference period end",
            "update frequency",
            "last modified fresh",
            "end date up to date",
            "organisation",
            "data link",
            "data type",
            "url",
            "is cod",
            "tags",
            "public",
            "requestable",
            "archived",
            "updated by cod script",
            "formerly updated by cod script",
            "updated by non-cod script",
            "date updated by script",
            "updated_by_script<<last_modified",
            "last_modified<<updated_by_script",
            "has quickcharts",
        )
    ]
    for dataset in downloads.get_all_datasets():
        datasetstats = DatasetStatistics(
            downloads.today,
            dataset_name_to_explorers,
            dataset_id_to_requests,
            last_modified_aging,
            end_date_aging,
            dataset,
        )
        if datasetstats.last_modified is None:
            continue
        dataset_id = dataset["id"]
        name = dataset["name"]
        title = dataset["title"]
        downloads_5years = dataset_downloads.get(dataset_id, 0)
        created = dataset["metadata_created"]
        metadata_updated = dataset["metadata_modified"]
        if not datasetstats.updated_by_script:
            year_month = created[:7]
            created_per_month[year_month] = created_per_month.get(year_month, 0) + 1
            year_month = metadata_updated[:7]
            metadata_updated_per_month[year_month] = (
                metadata_updated_per_month.get(year_month, 0) + 1
            )
            year_month = datasetstats.last_modified.isoformat()[:7]
            data_updated_per_month[year_month] = (
                data_updated_per_month.get(year_month, 0) + 1
            )
        update_frequency = dataset.get("data_update_frequency", "")
        org = dataset.get("organization")
        if org:
            org = org["title"]
        else:
            org = "NONE!"
        url = dataset.get_hdx_url()
        row = (
            name,
            title,
            dataset_id,
            downloads_5years,
            created,
            metadata_updated,
            datasetstats.last_modified,
            datasetstats.updated_last_3_months,
            datasetstats.updated_previous_qtr,
            datasetstats.startdate,
            datasetstats.enddate,
            update_frequency,
            datasetstats.last_modified_fresh,
            datasetstats.end_date_uptodate,
            org,
            datasetstats.data_link,
            datasetstats.data_type,
            url,
            datasetstats.is_cod,
            datasetstats.tags,
            datasetstats.public,
            datasetstats.requestable,
            datasetstats.archived,
            datasetstats.updated_by_cod_script,
            datasetstats.old_updated_by_cod_script,
            datasetstats.updated_by_noncod_script,
            datasetstats.updated_by_script,
            datasetstats.old_updated_by_noncod_script,
            datasetstats.outdated_lastmodified,
            datasetstats.has_quickcharts,
        )
        rows.append(row)
    if rows:
        filepath = join(output_dir, "datasets.csv")
        logger.info(f"Writing rows to {filepath}")
        write_list_to_csv(filepath, rows, headers=1, encoding="utf-8")
    keys = set(created_per_month.keys())
    keys.update(metadata_updated_per_month.keys())
    keys.update(data_updated_per_month.keys())
    rows = [("Year Month", "Created", "Metadata Updated", "Data Updated")]
    for key in sorted(keys):
        row = (
            key,
            created_per_month.get(key, ""),
            metadata_updated_per_month.get(key, ""),
            data_updated_per_month.get(key, ""),
        )
        rows.append(row)
    if rows:
        filepath = join(output_dir, "non_script_updates.csv")
        logger.info(f"Writing rows to {filepath}")
        write_list_to_csv(filepath, rows, headers=1, encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Datasets Info script")
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
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), Downloads
        ),
        downloads=downloads,
        output_dir=args.output_dir,
    )
