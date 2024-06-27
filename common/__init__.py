from datetime import datetime, timedelta, timezone

from hdx.utilities.dictandlist import dict_of_lists_add


def get_dataset_name_to_explorers(downloads):
    json = downloads.get_package_links()
    dataset_name_to_explorers = {}
    for explorergridlink in json["result"]:
        explorergrid = explorergridlink["title"]
        for dataset_name in set(explorergridlink["package_list"].split(",")):
            dict_of_lists_add(dataset_name_to_explorers, dataset_name, explorergrid)
    return dataset_name_to_explorers


def get_dataset_id_to_requests(downloads):
    dataset_id_to_requests = {}
    for request in downloads.get_requests():
        dict_of_lists_add(dataset_id_to_requests, request["package_id"], request)
    return dataset_id_to_requests


def get_freshness_by_frequency(downloads, url):
    yaml = downloads.get_aging(url)
    freshness_by_frequency = {}
    for key, value in yaml["aging"].items():
        update_frequency = int(key)
        freshness_frequency = {}
        for status in value:
            nodays = value[status]
            freshness_frequency[status] = timedelta(days=nodays)
        freshness_by_frequency[update_frequency] = freshness_frequency
    return freshness_by_frequency


def get_previous_quarter(date):
    if date.month < 4:
        start_date = datetime(date.year - 1, 10, 1, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(date.year - 1, 12, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)
    elif date.month < 7:
        start_date = datetime(date.year, 1, 1, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(date.year, 3, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)
    elif date.month < 10:
        start_date = datetime(date.year, 4, 1, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(date.year, 6, 30, 23, 59, 59, 999999, tzinfo=timezone.utc)
    else:
        start_date = datetime(date.year, 7, 1, 0, 0, tzinfo=timezone.utc)
        end_date = datetime(date.year, 9, 30, 23, 59, 59, 999999, tzinfo=timezone.utc)
    return start_date, end_date
