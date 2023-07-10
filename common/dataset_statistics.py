import logging
import re
from collections import UserDict
from datetime import datetime, timedelta

from dateutil.parser import ParserError
from dateutil.relativedelta import relativedelta
from hdx.api.configuration import Configuration
from hdx.utilities.dateparse import parse_date

logger = logging.getLogger(__name__)


class DatasetStatistics(UserDict):
    bracketed_date = re.compile(r"\((.*)\)")

    def __init__(
        self, today, dataset_name_to_explorers, freshness_by_frequency, dataset
    ):
        super().__init__(dataset.data)
        self.today = today
        self.last_quarter = today - relativedelta(months=3)
        self.dataset_name_to_explorers = dataset_name_to_explorers
        self.freshness_by_frequency = freshness_by_frequency
        self.dataset = dataset
        self.last_modified = None
        self.configuration = Configuration.read()
        self.get_status()
        self.get_cod()
        self.get_date_info()
        self.get_update_frequency_info()
        self.get_updated_by_script()
        self.get_in_explorer_or_grid()
        self.get_tags()
        self.get_freshness()

    def get_status(self):
        self.public = "N" if self["private"] else "Y"
        self.data_link = ""
        requestable = self.dataset.is_requestable()
        if requestable:
            self.requestable = "Y"
        else:
            self.requestable = "N"
            resources = self.dataset.get_resources()
            if resources:
                self.data_link = resources[0]["url"]
        self.archived = "Y" if self["archived"] else "N"

    def get_cod(self):
        cod_level = self.get("cod_level")
        if cod_level:
            self.is_cod = "Y"
        else:
            self.is_cod = "N"

    def get_date_info(self):
        last_modified = self.get("last_modified")
        if not last_modified:
            logger.error(f"Dataset {self['name']} has no last modified field!")
            self.last_modified = None
            self.updated_last_3_months = ""
            return
        self.last_modified = parse_date(last_modified, include_microseconds=True)
        if self.last_quarter < self.last_modified <= self.today:
            self.updated_last_3_months = "Y"
        else:
            self.updated_last_3_months = "N"
        reference_period = self.dataset.get_reference_period()
        self.startdate = reference_period["startdate_str"]
        if reference_period["ongoing"]:
            self.enddate = "ongoing"
        else:
            self.enddate = reference_period["enddate_str"]

    def get_updated_by_script(self):
        updated_by_script = self.get("updated_by_script")
        self.updated_by_script = None
        self.updated_by_noncod_script = "N"
        self.updated_by_cod_script = "N"
        self.old_updated_by_noncod_script = "N"
        self.outdated_lastmodified = "N"
        if not updated_by_script:
            return
        if self.public == "N" or self.requestable == "Y" or self.archived == "Y":
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
        if (
            "HDXINTERNAL" in updated_by_script
            and "CODs" in updated_by_script
            and "cod_level" in self.data
        ):
            self.updated_by_cod_script = "Y"
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

    def get_in_explorer_or_grid(self):
        if self["name"] in self.dataset_name_to_explorers:
            self.in_explorer_or_grid = "Y"
        else:
            self.in_explorer_or_grid = "N"

    def get_update_frequency_info(self):
        self.update_frequency = self.get("data_update_frequency", "")
        update_frequency = self.dataset.get_expected_update_frequency()
        if update_frequency == "Live":
            self.live = "Y"
        else:
            self.live = "N"
        reference_period = self.dataset.get_reference_period()
        if reference_period["ongoing"]:
            self.ongoing = "Y"
        else:
            self.ongoing = "N"

    def get_tags(self):
        tags = self.dataset.get_tags()
        self.tags = ", ".join(tags)

    def calculate_freshness(
        self, last_modified: datetime, update_frequency: int
    ) -> int:
        """Calculate freshness based on a last modified date and the expected update
        frequency. Returns 0 for fresh, 1 for due, 2 for overdue and 3 for delinquent.

        Args:
            last_modified (datetime): Last modified date
            update_frequency (int): Expected update frequency

        Returns:
            int: 0 for fresh, 1 for due, 2 for overdue and 3 for delinquent
        """
        delta = self.today - last_modified
        if delta >= self.freshness_by_frequency[update_frequency]["Delinquent"]:
            return "Delinquent"
        elif delta >= self.freshness_by_frequency[update_frequency]["Overdue"]:
            return "Overdue"
        elif delta >= self.freshness_by_frequency[update_frequency]["Due"]:
            return "Due"
        return "Fresh"

    def get_freshness(self):
        self.fresh = ""
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
                self.fresh = "Fresh"
            elif update_frequency == -1:
                self.fresh = "Fresh"
            elif update_frequency == -2:
                self.fresh = "Fresh"
            else:
                self.fresh = self.calculate_freshness(
                    latest_of_modifieds, update_frequency
                )
