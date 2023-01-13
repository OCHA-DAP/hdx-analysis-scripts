from os import getenv

from hdx.utilities.loader import load_yaml
from mixpanel_utils import MixpanelUtils


def get_mixpanel_downloads(mixpanel_config_yaml, start_date, end_date):
    try:
        mixpanel_config = load_yaml(mixpanel_config_yaml)
        api_secret = mixpanel_config["api_secret"]
        project_id = mixpanel_config["project_id"]
        token = mixpanel_config["token"]
    except FileNotFoundError:
        api_secret = getenv("MIXPANEL_API_SECRET")
        project_id = getenv("MIXPANEL_PROJECT_ID")
        token = getenv("MIXPANEL_TOKEN")
    mputils = MixpanelUtils(
        api_secret=api_secret,
        project_id=project_id,
        token=token,
    )
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    jql_query = """
    function main() {
      return Events({
        from_date: '%s',
        to_date: '%s',
        event_selectors: [{event: "resource download"}]
      })
      .groupByUser(["properties.resource id","properties.dataset id",mixpanel.numeric_bucket('time',mixpanel.daily_time_buckets)],mixpanel.reducer.null())
      .groupBy(["key.2"], mixpanel.reducer.count())
        .map(function(r){
        return [
          r.key[0], r.value
        ];
      });
    }""" % (
        start_date_str,
        end_date_str,
    )
    return dict(mputils.query_jql(jql_query))
