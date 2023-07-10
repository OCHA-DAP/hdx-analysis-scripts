from hdx.utilities.dictandlist import dict_of_lists_add


def get_dataset_name_to_explorers(downloads):
    json = downloads.get_package_links()
    dataset_name_to_explorers = dict()
    for explorergridlink in json["result"]:
        explorergrid = explorergridlink["title"]
        for dataset_name in set(explorergridlink["package_list"].split(",")):
            dict_of_lists_add(dataset_name_to_explorers, dataset_name, explorergrid)
    return dataset_name_to_explorers
