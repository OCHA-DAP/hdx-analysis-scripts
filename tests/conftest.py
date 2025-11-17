from os.path import isfile, join

import pytest
from dateutil.relativedelta import relativedelta

from hdx.analysis_scripts.common.downloads import Downloads
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.dateparse import parse_date
from hdx.utilities.loader import load_json, load_yaml
from hdx.utilities.path import script_dir_plus_file
from hdx.utilities.useragent import UserAgent


@pytest.fixture(scope="session")
def configuration():
    Configuration._create(
        hdx_read_only=True,
        hdx_site="prod",
        user_agent="test",
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), Downloads
        ),
    )
    UserAgent.set_global("test")
    return Configuration.read()


@pytest.fixture(scope="session")
def fixtures():
    return join("tests", "fixtures")


@pytest.fixture(scope="session")
def input_folder(fixtures):
    return join(fixtures, "input")


@pytest.fixture(scope="session")
def mock_downloads(input_folder):
    class MockDownloads:
        today = parse_date("2025-11-16 22:50:00")

        @classmethod
        def set_api_key(cls, api_key):
            pass

        @classmethod
        def get_mixpanel_downloads(cls, months_ago):
            end_date = cls.today
            start_date = end_date - relativedelta(months=months_ago)
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")
            filename = Downloads.mixpanel_file.replace(
                ".json", f"_{start_date_str}-{end_date_str}.json"
            )
            return load_json(join(input_folder, filename))

        @staticmethod
        def get_all_datasets():
            n = 0
            dataset_dict_list = []
            while 1:
                filename = Downloads.datasets_file.replace(".json", f"_{n}.json")
                path = join(input_folder, filename)
                if not isfile(path):
                    break
                dataset_dict_list.extend(load_json(path))
                n += 1

            dataset_list = []
            for dataset_dict in dataset_dict_list:
                dataset = Dataset()
                dataset.data = dataset_dict
                try:
                    dataset.separate_resources()
                except KeyError:
                    pass
                dataset_list.append(dataset)
            return dataset_list

        @staticmethod
        def get_geospatiality_locations(url):
            geospatiality = load_json(join(input_folder, Downloads.geospatiality_file))
            locations = load_json(join(input_folder, Downloads.locations_file))
            return geospatiality, locations

        @staticmethod
        def get_package_links():
            return load_json(join(input_folder, Downloads.packagelinks_file))

        @staticmethod
        def get_requests():
            return load_json(join(input_folder, Downloads.hdxconnect_file))

        @staticmethod
        def get_all_organisations():
            return load_json(join(input_folder, Downloads.organisations_file))

        @staticmethod
        def get_all_users():
            return load_json(join(input_folder, Downloads.users_file))

        @staticmethod
        def get_aging(url):
            return load_yaml(join(input_folder, Downloads.aging_file))

    return MockDownloads()
