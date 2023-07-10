from datetime import timedelta

from hdx.utilities.dictandlist import dict_of_lists_add


def get_dataset_name_to_explorers(downloads):
    json = downloads.get_package_links()
    dataset_name_to_explorers = dict()
    for explorergridlink in json["result"]:
        explorergrid = explorergridlink["title"]
        for dataset_name in set(explorergridlink["package_list"].split(",")):
            dict_of_lists_add(dataset_name_to_explorers, dataset_name, explorergrid)
    return dataset_name_to_explorers


def get_freshness_by_frequency(downloads, url):
    yaml = downloads.get_aging(url)
    freshness_by_frequency = dict()
    for key, value in yaml["aging"].items():
        update_frequency = int(key)
        freshness_frequency = dict()
        for status in value:
            nodays = value[status]
            freshness_frequency[status] = timedelta(days=nodays)
        freshness_by_frequency[update_frequency] = freshness_frequency
    return freshness_by_frequency
