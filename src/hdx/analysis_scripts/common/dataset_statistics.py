import logging
import re
from collections import UserDict
from datetime import datetime, timedelta

from dateutil.parser import ParserError
from dateutil.relativedelta import relativedelta

from hdx.analysis_scripts.common import get_previous_quarter
from hdx.api.configuration import Configuration
from hdx.utilities.dateparse import parse_date

logger = logging.getLogger(__name__)


class DatasetStatistics(UserDict):
    bracketed_date = re.compile(r"\((.*)\)")

    def __init__(
        self,
        today,
        dataset_name_to_explorers,
        dataset_id_to_requests,
        last_modified_aging,
        end_date_aging,
        dataset,
    ):
        super().__init__(dataset.data)
        self.today = today
        self.last_3_months = today - relativedelta(months=3)
        self.previous_quarter = get_previous_quarter(today)
        self.dataset_name_to_explorers = dataset_name_to_explorers
        self.dataset_id_to_requests = dataset_id_to_requests
        self.last_modified_aging = last_modified_aging
        self.end_date_aging = end_date_aging
        self.dataset = dataset
        self.last_modified = None
        self.configuration = Configuration.read()
        self.get_status()
        self.get_cod()
        self.get_date_info()
        self.get_update_frequency_info()
        self.get_in_explorer_or_grid()
        self.get_requests()
        self.crisis_tag = False
        self.get_tags()
        self.get_updated_by_script()
        self.get_last_modified_freshness()
        self.get_end_date_freshness()

    def get_status(self):
        self.public = "N" if self["private"] else "Y"
        self.internal_resources = 0
        self.external_resources = 0
        self.data_link = ""
        self.data_type = ""
        requestable = self.dataset.is_requestable()
        if requestable:
            self.requestable = "Y"
        else:
            self.requestable = "N"
            resources = self.dataset.get_resources()
            if resources:
                resource = resources[0]
                self.data_link = resource["url"]
                self.data_type = resource["url_type"]
                for resource in resources:
                    if resource["url_type"] == "api":
                        self.external_resources += 1
                    else:
                        self.internal_resources += 1
        self.archived = "Y" if self["archived"] else "N"
        if self.public == "N" or self.requestable == "Y" or self.archived == "Y":
            self.exclude_from_stats = "Y"
        else:
            self.exclude_from_stats = "N"

    def get_cod(self):
        cod_level = self.get("cod_level")
        if cod_level:
            self.is_cod = "Y"
        else:
            self.is_cod = "N"

    def get_date_info(self):
        self.created = parse_date(self["metadata_created"], include_microseconds=True)
        try:
            time_period = self.dataset.get_time_period()
        except ParserError:
            time_period = None
        if time_period:
            self.startdate = time_period["startdate_str"]
            if time_period["ongoing"]:
                self.enddate = "ongoing"
            else:
                self.enddate = time_period["enddate_str"]
        else:
            self.startdate = ""
            self.enddate = ""
            logger.error(f"Dataset {self['name']} has no time period!")
        last_modified = self.get("last_modified")
        if not last_modified:
            logger.error(f"Dataset {self['name']} has no last modified field!")
            self.last_modified = None
            self.updated_last_3_months = ""
            return
        self.last_modified = parse_date(last_modified, include_microseconds=True)
        if self.last_3_months < self.last_modified <= self.today:
            self.updated_last_3_months = "Y"
        else:
            self.updated_last_3_months = "N"
        if self.previous_quarter[0] <= self.last_modified <= self.previous_quarter[1]:
            self.updated_previous_qtr = "Y"
        else:
            self.updated_previous_qtr = "N"

    def get_update_frequency_info(self):
        self.update_frequency = self.get("data_update_frequency", "")
        update_frequency = self.dataset.get_expected_update_frequency()
        if update_frequency == "Live":
            self.live = "Y"
        else:
            self.live = "N"
        try:
            time_period = self.dataset.get_time_period()
        except ParserError:
            time_period = None
        if time_period:
            if time_period["ongoing"]:
                self.ongoing = "Y"
            else:
                self.ongoing = "N"
        else:
            self.ongoing = ""

    def get_in_explorer_or_grid(self):
        if self["name"] in self.dataset_name_to_explorers:
            self.in_explorer_or_grid = "Y"
        else:
            self.in_explorer_or_grid = "N"

    def get_requests(self):
        self.new_requests = 0
        self.open_requests = 0
        self.archived_requests = 0
        self.shared_requests = 0
        self.denied_requests = 0
        for request in self.dataset_id_to_requests.get(self["id"], []):
            if request["state"] == "new":
                self.new_requests += 1
            elif request["state"] == "open":
                self.open_requests += 1
            else:
                self.archived_requests += 1
                if request["data_shared"]:
                    self.shared_requests += 1
                elif request["rejected"]:
                    self.denied_requests += 1

    def get_tags(self):
        tags = self.dataset.get_tags()
        self.tags = ", ".join(tags)
        for tag in tags:
            if tag[:7] == "crisis-":
                self.crisis_tag = True

    def add_tags_to_set(self, tagset):
        tags = self.dataset.get_tags()
        tagset.update(tags)

    def get_updated_by_script(self):
        updated_by_script = self.get("updated_by_script")
        self.updated_by_script = None
        self.updated_by_noncod_script = "N"
        self.updated_by_cod_script = "N"
        self.old_updated_by_noncod_script = "N"
        self.old_updated_by_cod_script = "N"
        self.outdated_lastmodified = "N"
        if not updated_by_script:
            return
        if self.exclude_from_stats == "Y":
            return
        if "HDXINTERNAL" in updated_by_script:
            if any(x in updated_by_script for x in ("tagbot",)):
                return
        if any(
            x in updated_by_script
            for x in (
                "HDXPythonLibrary/5.5.6-test (2022-03-15",
                "HDXPythonLibrary/5.4.8-test (2022-01-04",
                "HDXPythonLibrary/5.4.1-test (2021-11-17",
            )
        ):  # Mike maintainer bulk change
            return
        match = self.bracketed_date.search(updated_by_script)
        if match is None:
            return
        else:
            try:
                self.updated_by_script = parse_date(
                    match.group(1), include_microseconds=True
                )
            except ParserError:
                return
        if "HDXINTERNAL" in updated_by_script and "CODs" in updated_by_script:
            if "cod_level" in self.data:
                self.updated_by_cod_script = "Y"
            else:
                # no longer updated by COD script
                self.old_updated_by_cod_script = "Y"
            return

        if self.last_modified:
            if self.updated_by_script > self.last_modified:
                self.updated_by_noncod_script = "Y"
                update_frequency = self.dataset.get_expected_update_frequency()
                if update_frequency != "Live":
                    difference = self.updated_by_script - self.last_modified
                    if difference > timedelta(hours=1):
                        self.outdated_lastmodified = "Y"
                return
            difference = self.last_modified - self.updated_by_script
            if difference < timedelta(hours=1):
                self.updated_by_noncod_script = "Y"
            else:
                self.old_updated_by_noncod_script = "Y"

    def calculate_lm_freshness(
        self, last_modified: datetime, update_frequency: int
    ) -> str:
        """Calculate freshness based on a last modified date and the expected update
        frequency. Returns "Fresh", "Due", "Overdue" or "Delinquent".

        Args:
            last_modified (datetime): Last modified date
            update_frequency (int): Expected update frequency

        Returns:
            str: "Fresh", "Due", "Overdue" or "Delinquent"
        """
        delta = self.today - last_modified
        if delta >= self.last_modified_aging[update_frequency]["Delinquent"]:
            return "Delinquent"
        elif delta >= self.last_modified_aging[update_frequency]["Overdue"]:
            return "Overdue"
        elif delta >= self.last_modified_aging[update_frequency]["Due"]:
            return "Due"
        return "Fresh"

    def get_last_modified_freshness(self):
        self.last_modified_fresh = ""
        if self.exclude_from_stats == "Y":
            return
        if not self.last_modified:
            return
        review_date = self.get("review_date")
        if review_date is None:
            latest_of_modifieds = self.last_modified
        else:
            review_date = parse_date(review_date, include_microseconds=True)
            if review_date > self.last_modified:
                latest_of_modifieds = review_date
            else:
                latest_of_modifieds = self.last_modified
        if self.updated_by_script and self.updated_by_script > latest_of_modifieds:
            latest_of_modifieds = self.updated_by_script
        if self.update_frequency:
            update_frequency = int(self.update_frequency)
            if update_frequency == 0:
                self.last_modified_fresh = "Fresh"
            elif update_frequency == -1:
                self.last_modified_fresh = "Fresh"
            elif update_frequency == -2:
                self.last_modified_fresh = "Fresh"
            else:
                self.last_modified_fresh = self.calculate_lm_freshness(
                    latest_of_modifieds, update_frequency
                )

    def calculate_ed_uptodate(self, end_date: datetime, update_frequency: int) -> str:
        """Calculate up to date based on time period end date and the expected
        update frequency. Returns "UpToDate" or "OutOfDate".

        Args:
            last_modified (datetime): Last modified date
            update_frequency (int): Expected update frequency

        Returns:
            str: "UpToDate" or "OutOfDate"
        """
        delta = self.today - end_date
        if delta >= self.end_date_aging[update_frequency]["OutOfDate"]:
            return "OutOfDate"
        return "UpToDate"

    def get_end_date_freshness(self):
        self.end_date_uptodate = ""
        if self.exclude_from_stats == "Y":
            return
        if self.update_frequency:
            update_frequency = int(self.update_frequency)
            if update_frequency < 0:
                return
            if update_frequency == 0:
                self.end_date_uptodate = "UpToDate"
            elif update_frequency > 0:
                if self.enddate == "ongoing":
                    self.end_date_uptodate = "UpToDate"
                    return
                enddate = parse_date(self.enddate)
                self.end_date_uptodate = self.calculate_ed_uptodate(
                    enddate, update_frequency
                )
