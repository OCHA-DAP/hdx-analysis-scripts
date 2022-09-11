from os.path import expanduser, join

from hdx.data.dataset import Dataset
from hdx.facades.simple import facade
from hdx.utilities.dictandlist import write_list_to_csv
from hdx.utilities.loader import load_yaml
from mixpanel_utils import MixpanelUtils


def main():
    home_folder = expanduser("~")
    configuration = load_yaml(join(home_folder, ".mixpanel.yml"))
    mputils = MixpanelUtils(
        configuration["api_secret"],
        project_id=configuration["project_id"],
        token=configuration["token"],
    )

    jql_query = """
    function main() {
      return Events({
        from_date: '2017-01-01',
        to_date: '2022-12-31',
        event_selectors: [{event: "resource download"}]
      })
      .groupByUser(["properties.resource id","properties.dataset id",mixpanel.numeric_bucket('time',mixpanel.daily_time_buckets)],mixpanel.reducer.null())
      .groupBy(["key.2"], mixpanel.reducer.count())
        .map(function(r){
        return [
          r.key[0], r.value
        ];
      });
    }"""

    dataset_downloads = dict(mputils.query_jql(jql_query))
    datasets = Dataset.get_all_datasets()
    rows = [
        (
            "name",
            "title",
            "downloads since 2017",
            "downloads all time",
            "date created",
            "date metadata updated",
            "date data updated",
            "dataset start date",
            "dataset end date",
            "update frequency",
            "organisation",
            "data link",
            "url",
            "is cod",
            "tags",
            "private",
            "requestable",
            "updated by script",
            "archived",
        )
    ]
    for dataset in datasets:
        dataset_id = dataset["id"]
        name = dataset["name"]
        title = dataset["title"]
        downloads_year = dataset_downloads.get(dataset_id, 0)
        downloads_alltime = dataset.get("total_res_downloads", "")
        created = dataset["metadata_created"]
        metadata_updated = dataset["metadata_modified"]
        data_updated = dataset["last_modified"]
        date_of_dataset = dataset.get_date_of_dataset()
        startdate = date_of_dataset["startdate_str"]
        if date_of_dataset["ongoing"]:
            enddate = "ongoing"
        else:
            enddate = date_of_dataset["enddate_str"]
        update_frequency = dataset.get("data_update_frequency", "")
        org = dataset.get("organization")
        if org:
            org = org["title"]
        else:
            org = "NONE!"
        requestable = dataset.is_requestable()
        if requestable:
            data_link = ""
            requestable = "Y"
        else:
            data_link = dataset.get_resource()["url"]
            requestable = "N"
        url = dataset.get_hdx_url()
        tags = dataset.get_tags()
        if "common operational dataset - cod" in tags:
            is_cod = "Y"
        else:
            is_cod = "N"
        tags = ", ".join(tags)
        private = "Y" if dataset["private"] else "N"
        updated_by_script = dataset.get("updated_by_script", "")
        archived = "Y" if dataset["archived"] else "N"
        row = (
            name,
            title,
            downloads_year,
            downloads_alltime,
            created,
            metadata_updated,
            data_updated,
            startdate,
            enddate,
            update_frequency,
            org,
            data_link,
            url,
            is_cod,
            tags,
            private,
            requestable,
            updated_by_script,
            archived,
        )
        rows.append(row)
    write_list_to_csv("datasets.csv", rows, headers=1)


if __name__ == "__main__":
    facade(main, hdx_site="prod", user_agent="test")
